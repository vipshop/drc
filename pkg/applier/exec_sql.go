// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package applier

import (
	"context"
	"database/sql"
	"strings"
	"unicode"

	"fmt"
	"net"

	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-sql-driver/mysql"
	"github.com/vipshop/drc/pkg/utils"
)

type ExecOpt struct {
	db               *sql.DB
	allowDDL         bool
	strategyType     StrategyType
	updateTimeColumn string

	serverId      uint32
	checkServerId bool

	kafkaTopic string
	lwmOffset  int64

	resultC chan ExecResult
}

type ExecResult struct {
	Err error  `json:"-"`
	Msg string `json:"msg"`
	Trx *Trx   `json:"trx"`
}

// 判断是否非法事务
func isIllegal(opts *ExecOpt, trx *Trx) bool {
	if len(trx.Events) == 0 {
		// 单元测试无events
		return false
	}
	if trx.Events[0].SqlStatement != "" {
		// 根据DDL流向选项判断能否执行DDL
		if !opts.allowDDL {
			// 非法事务
			return true

		}
	}
	return false
}

func ExecBinlogTrx(ctx context.Context, opts *ExecOpt, binlogTrx *Trx) {
	var err error
	var executableSQLs []*ExecutableSQL
	execResult := ExecResult{
		Err: nil,
		Msg: "",
		Trx: binlogTrx,
	}

	defer func() {
		if err != nil {
			execResult.Err = err
			execResult.Msg = err.Error()
		}
		opts.resultC <- execResult
	}()

	if opts == nil || binlogTrx == nil {
		log.Errorf("ExecBinlogTrx: args error,opts:%v,binlogTrx:%v", opts, binlogTrx)
		err = ErrArgsIsNil
		return
	}

	if isIllegal(opts, binlogTrx) {
		log.Errorf("ExecBinlogTrx: illegal trx %s", binlogTrx.Gtid)
		err = ErrIllegalTrx
		return
	}

	// 构造SQL
	executableSQLs, err = binlogTrx.BuildTransactionSQL()
	if err != nil {
		log.Errorf("ExecBinlogTrx:BuildTransactionSQL error,err=%s,binlogTrx=%v", err, *binlogTrx)
		return
	}

	//执行事务包含的SQL
	isInvalidConn := false
	for i := 0; ; i++ {
		if isInvalidConn {
			// 如果上一次错误是由于连接异常, 需要检查serverId
			opts.checkServerId = true
		} else {
			opts.checkServerId = false
		}

		isInvalidConn = false
		err = execSQLs(ctx, opts, binlogTrx, executableSQLs)

		if err == nil {
			if 0 < i {
				log.Warnf("ExecBinlogTrx:execSQLs exec success, i:%d, trx:%v", i, binlogTrx)
			}
			return
		} else if err != nil {
			log.Warnf("ExecBinlogTrx:execSQLs exec error: %s, error type: %T", err, err)

			if err == ErrServerIdChange {
				// server_id 发生改变, 需要退出
				log.Panicf("ExecBinlogTrx:execSQLs server_id changed, need to panic")
			}

			if err == mysql.ErrInvalidConn {
				isInvalidConn = true
			}

			if _, ok := err.(*net.OpError); ok {
				isInvalidConn = true
			}

			needRetry := false
			sleepDuration := time.Second * time.Duration(i+1) * 5

			if isInvalidConn {
				// 发生连接错误, 一直重试
				needRetry = true

				if i >= 10 && i%10 == 0 {
					// 每十次触发一次告警
					msg := fmt.Sprintf("mysql connection is invalid now: %s, will retry after %s", err, sleepDuration)
					utils.Alarm(msg)
				}
			}

			mysqlErr, ok := err.(*mysql.MySQLError)
			if ok && mysqlErr.Number == 1213 && i < 8 {
				//发生死锁，重试
				needRetry = true
			}

			if needRetry {
				log.Warnf("ExecBinlogTrx:execSQLs error will retry the %dth time after %s", i+1, sleepDuration)
				time.Sleep(sleepDuration)
				continue
			}

			// 其他错误不重试
			log.Errorf("ExecBinlogTrx:execSQLs error, err: %s, opt=%v, binlogTrx=%v", err, *opts, *binlogTrx)
			return
		}
	}

	return
}

func getServerId(tx *sql.Tx) (uint32, error) {
	var serverId uint32
	sql := "SELECT @@server_id"
	err := tx.QueryRow(sql).Scan(&serverId)
	if err != nil {
		return 0, err
	}

	return serverId, nil
}

//执行事务包含的SQL
func execSQLs(ctx context.Context, opts *ExecOpt, binlogTrx *Trx, sqls []*ExecutableSQL) error {
	if len(sqls) == 0 {
		log.Warningf("execSQLs:len(sqls) is 0,opts:%v", *opts)
		return nil
	}

	tx, err := opts.db.Begin()
	if err != nil {
		return err
	}
	//出错都需要调用rollback函数，回滚事务
	rollback := func(err error) error {
		tx.Rollback()
		return err
	}

	if opts.checkServerId {
		// 需要检查server_id
		log.Warn("execSQLs: need to check server_id")
		serverId, err := getServerId(tx)
		if err != nil {
			log.Errorf("execSQLs: get mysql server_id errors: %s", err)
			return rollback(err)
		}
		if serverId != opts.serverId {
			log.Errorf("execSQLs: mysql server_id has changed, current is: %d, expected is: %d", serverId, opts.serverId)
			return rollback(ErrServerIdChange)
		}
	}

	//执行标记SQL
	markSQL := buildMarkSQL(binlogTrx)
	res, err := tx.Exec(markSQL)
	if err != nil {
		log.Errorf("execSQLs:Exec markSQL error,query:%s,err:%s", markSQL, err)
		return rollback(err)
	}
	rowAffected, err := res.RowsAffected()
	if err != nil {
		log.Errorf("execSQLs:Exec markSQL RowsAffected error,query:%s,err:%s", markSQL, err)
		return rollback(err)
	}
	if rowAffected != 1 {
		log.Errorf("execSQLs:Exec markSQL rowAffected is not 1,query:%s,rowAffected:%d", markSQL, rowAffected)
		return rollback(ErrUpdateCheckpointTable)
	}

	for _, s := range sqls {
		select {
		case <-ctx.Done():
			return rollback(ctx.Err())
		default:
		}
		//根据类型执行SQL
		switch s.EventType {
		case DELETE_ROWS_EVENT:
			if err := s.execDelete(tx, opts.strategyType, opts.updateTimeColumn); err != nil {
				log.Errorf("execSQLs:exec delete sql error,err:%s,query:%s,args:%+v",
					err, s.Query, s.Args)
				return rollback(err)
			}
		case UPDATE_ROWS_EVENT:
			if err := s.execUpdate(tx, opts.strategyType, opts.updateTimeColumn); err != nil {
				log.Errorf("execSQLs:exec update sql error,err:%s,query:%s,args:%+v",
					err, s.Query, s.Args)
				return rollback(err)
			}
		case WRITE_ROWS_EVENT:
			if err := s.execInsert(tx, opts.strategyType, opts.updateTimeColumn); err != nil {
				log.Errorf("execSQLs:exec insert sql error,err:%s,query:%s,args:%+v",
					err, s.Query, s.Args)
				return rollback(err)
			}
		case QUERY_EVENT:
			if err := s.execDDL(tx); err != nil {
				log.Errorf("execSQLs:exec ddl sql error,err:%s,query:%s",
					err, s.Query)
				return rollback(err)
			}
		default:
			log.Errorf("execSQLs:sql type is not expectable, query:%s,eventType:%v", s.Query, s.EventType)
			return rollback(ErrEventType)
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

//执行delete
func (s *ExecutableSQL) execDelete(tx *sql.Tx, strategy StrategyType, updateTimeColumn string) error {
	res, err := tx.Exec(s.Query, s.Args...)
	if err != nil {
		log.Errorf("execDelete:Exec error,err:%s,query:%s,args:%+v", err, s.Query, s.Args)
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Errorf("execDelete:RowsAffected error,err:%s,query:%s,args:%+v", err, s.Query, s.Args)
		return err
	}
	switch count {
	case 1:
		//成功执行
		log.Debugf("execDelete:exec query successfully, query:%s,args:%+v,", s.Query, s.Args)
		return nil
	case 0:
		//输出SQL
		rewriteQuery, err := generateSQL(s.Query, s.Args)
		if err != nil {
			log.Errorf("execDelete:generateSQL error,err:%s,query:%s", err, s.Query)
		} else {
			log.Warnf("execDelete:conflict sql:%s", rewriteQuery)
		}
		//有冲突
		err = s.HandDeleteConflict(tx, strategy, updateTimeColumn)
		if err != nil {
			return err
		}
		deleteConflictCount.Inc(1)
		return nil
	default:
		//非预期情况
		log.Errorf("execDelete:Exec delete query,count is not 1,count:%d,query:%s,args:%+v", count, s.Query, s.Args)
		return ErrrUnexpect
	}
	return nil
}

//执行update
func (s *ExecutableSQL) execUpdate(tx *sql.Tx, strategy StrategyType, updateTimeColumn string) error {
	res, err := tx.Exec(s.Query, s.Args...)
	if err != nil {
		log.Errorf("execUpdate:Exec query error,err:%s,query:%s,args:%+v", err, s.Query, s.Args)
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Errorf("execUpdate:RowsAffected error,err:%s,query:%s,args:%+v", err, s.Query, s.Args)
		return err
	}
	switch count {
	case 1:
		//成功执行
		//log.Debugf("execUpdate:exec query successfully, query:%s,args:%+v,", s.Query, s.Args)
		return nil
	case 0:
		//输出SQL
		rewriteQuery, err := generateSQL(s.Query, s.Args)
		if err != nil {
			log.Errorf("execUpdate: generateSQL error,err:%s,query:%s", err, s.Query)
		} else {
			log.Warnf("execUpdate: conflict sql:%s", rewriteQuery)
		}
		//有冲突
		err = s.HandleUpdateConflict(tx, strategy, updateTimeColumn)
		if err != nil {
			return err
		}
		updateConflictCount.Inc(1)
		return nil
	default:
		log.Errorf("execUpdate:Exec update query,count is not 1,count:%d,query:%s,args:%+v", count, s.Query, s.Args)
		return ErrrUnexpect
	}
	return nil
}

//执行insert
func (s *ExecutableSQL) execInsert(tx *sql.Tx, strategy StrategyType, updateTimeColumn string) error {
	_, err := tx.Exec(s.Query, s.Args...)
	if err != nil {
		mysqlerr, ok := err.(*mysql.MySQLError)
		//插入数据冲突
		if ok && mysqlerr.Number == 1062 {
			//输出SQL
			rewriteQuery, err := generateSQL(s.Query, s.Args)
			if err != nil {
				log.Errorf("execInsert: generateSQL error,err:%s,query:%s", err, s.Query)
			} else {
				log.Warnf("execInsert: conflict sql:%s", rewriteQuery)
			}
			err = s.HandInsertConflict(tx, strategy, updateTimeColumn)
			if err != nil {
				log.Errorf("execInsert:Exec error, err: %s, query:%s, args:%+v", err, s.Query, s.Args)
				return err
			}
			insertConflictCount.Inc(1)
			return nil
		} else {
			log.Errorf("execInsert:Exec error,err:%s,query:%s,args:%+v", err, s.Query, s.Args)
			return err
		}
	}
	log.Debugf("execInsert:exec query successfully, query:%s,args:%+v,", s.Query, s.Args)
	return nil
}

//执行DDL
func (s *ExecutableSQL) execDDL(tx *sql.Tx) error {
	if len(s.DatabaseName) != 0 {
		//在MySQL中执行CREATE/DROP DATABASE,CREATE/DROP SCHEMA时
		//产生的Query Event中会设置database，但这个database有可能不存在。
		//这导致Applier中执行use db，有可能报db不存在的错误，需要忽略该错误。
		useDBSQL := fmt.Sprintf("use `%s`", s.DatabaseName)
		_, err := tx.Exec(useDBSQL)
		if err != nil {
			mysqlerr, ok := err.(*mysql.MySQLError)
			if !ok || mysqlerr.Number != 1049 {
				// 如果不是unknown database，需要报错
				log.Errorf("ExecutableSQL.execDDL: exec query error,err:%s,query:%s", err, useDBSQL)
				return err
			}
			// 进一步判断是否要报错
			query := strings.TrimLeftFunc(s.Query, unicode.IsSpace)
			query = strings.TrimPrefix(query, "/*mysql-applier*/")
			query = strings.TrimLeftFunc(query, unicode.IsSpace)
			query = strings.ToUpper(query)
			if !strings.HasPrefix(query, "CREATE DATABASE") &&
				!strings.HasPrefix(query, "CREATE SCHEMA") &&
				!strings.HasPrefix(query, "DROP DATABASE") &&
				!strings.HasPrefix(query, "DROP SCHEMA") {
				// 如果不是这些语句，执行USE失败要报错
				log.Errorf("ExecutableSQL.execDDL: exec query unkown database error,err:%s,query:%s", err, useDBSQL)
				return err
			}
		}
	}

	log.Infof("ExecutableSQL.execDDL: executing %s", s.Query)
	_, err := tx.Exec(s.Query)
	if err != nil {
		log.Errorf("ExecutableSQL.execDDL: exec query error,err:%s,query:%s", err, s.Query)
		return err
	}
	return nil
}

//生成标记SQL
func buildMarkSQL(binlogTrx *Trx) string {
	markSQL := fmt.Sprintf("update %s.%s set `binlog_file_name`= '%s',`position`=%d, "+
		"`kafka_topic`='%s', `kafka_offset`=%d, `lwm_offset`=%d where id=%d",
		CheckpointDatabase, CheckpointTable, binlogTrx.BinlogFileName, binlogTrx.Position,
		binlogTrx.Topic, binlogTrx.BeginOffset, binlogTrx.LwmOffset, binlogTrx.Seq%CheckpointTableCount)
	return markSQL
}
