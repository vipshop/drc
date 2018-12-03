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
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/vipshop/drc/pkg/status"
	"github.com/vipshop/drc/pkg/utils"

	"strconv"

	log "github.com/Sirupsen/logrus"
)

const (
	StatusKey_ApplyProgress = "apply_progress"
	StatusKey_FailedTrx     = "failed_trx"
)
const (
	CheckpointTableCount = 1000
)

type Coordinator struct {
	config *ApplierConfig
	db     *sql.DB

	// 新trx到来信号
	trxC <-chan *Trx
	// 重试trx信号
	retryC <-chan string
	// 跳过trx信号
	skipC <-chan string

	// 通知内部协程退出
	cancel context.CancelFunc
	// 内部协程已退出信号
	quitC   chan struct{}
	started bool

	// 当前等待调度的Trx
	// 需加锁保护
	pendingTrx *Trx

	// 上一个调度的事务
	lastScheduledTrx *Trx

	// 正在运行的事务数量
	runningTrxs uint64
	// 已经失败的事务数量
	failedTrxs uint64

	execFunc func(context.Context, *ExecOpt, *Trx)
	// 事务执行结果
	trxResultC chan ExecResult

	// 取消事务执行
	cancelExecCtx  context.Context
	cancelExecFunc context.CancelFunc

	// apply进度
	// 需加锁保护
	progress ApplyProgress

	// 需要等待重试的事务
	// 需加锁保护
	waitToHandle map[string]ExecResult

	caq *AssignedQueue

	//目标数据的server_id
	serverId uint32

	l *sync.Mutex
}

type ApplyProgress struct {
	// 已经成功执行的offset
	Offset         int64  `json:"kafka_offset"`
	Gtid           string `json:"gtid"`
	Position       uint64 `json:"binlog_file_position"`
	BinlogFileName string `json:"binlog_file_name"`
}

func NewCoordinator(config *ApplierConfig, trxC <-chan *Trx, retryC <-chan string, skipC <-chan string) *Coordinator {
	o := &Coordinator{
		config:       config,
		trxC:         trxC,
		retryC:       retryC,
		skipC:        skipC,
		quitC:        make(chan struct{}),
		execFunc:     ExecBinlogTrx,
		trxResultC:   make(chan ExecResult, 10000),
		waitToHandle: make(map[string]ExecResult),
		caq:          newAssignedQueue(4096),
		l:            new(sync.Mutex),
	}

	o.cancelExecCtx, o.cancelExecFunc = context.WithCancel(context.Background())
	o.progress = ApplyProgress{
		Offset: -1, // 表示还没有执行任何事务
	}
	return o
}

// 向外部暴露apply progresss
func (o *Coordinator) getProgress() ApplyProgress {
	o.l.Lock()
	defer o.l.Unlock()
	return o.progress
}
func (o *Coordinator) setProgress(trx *Trx) {
	o.l.Lock()
	defer o.l.Unlock()
	o.progress.Offset = trx.EndOffset
	o.progress.Gtid = trx.Gtid
	o.progress.Position = trx.Position
	o.progress.BinlogFileName = trx.BinlogFileName
}

// 向外部暴露当前等待调度的trx
func (o *Coordinator) getPendingTrx() *Trx {
	o.l.Lock()
	defer o.l.Unlock()
	return o.pendingTrx
}
func (o *Coordinator) setPendingTrx(trx *Trx) {
	o.l.Lock()
	defer o.l.Unlock()
	o.pendingTrx = trx
}

// 向外暴露当前等待重试的trx
func (o *Coordinator) getWaitToHandle() map[string]ExecResult {
	o.l.Lock()
	defer o.l.Unlock()
	return o.waitToHandle
}
func (o *Coordinator) addWaitToHandle(gtid string, result ExecResult) {
	// 触发告警
	msg := fmt.Sprintf("Exec trx error: %s, gtid: %s", result.Msg, result.Trx.Gtid)
	utils.Alarm(msg)
	o.l.Lock()
	defer o.l.Unlock()
	o.waitToHandle[gtid] = result
}

// 将指定的事务从waitToHandle中剔除
func (o *Coordinator) delWaitToHandle(gtid string) {
	o.l.Lock()
	defer o.l.Unlock()
	delete(o.waitToHandle, gtid)
}

// 注册一些需要向其他模块暴露的status
func (o *Coordinator) registerStatus() {
	func1 := func() interface{} {
		return o.getProgress()
	}
	func2 := func() interface{} {
		return o.getWaitToHandle()
	}
	status.Register(StatusKey_ApplyProgress, func1)
	status.Register(StatusKey_FailedTrx, func2)
}

//创建库和表
func (o *Coordinator) initCheckpointTable() error {
	createDBSQL := fmt.Sprintf("/*mysql-applier*/CREATE DATABASE IF NOT EXISTS %s", CheckpointDatabase)
	createTableSQL := fmt.Sprintf(`
		/*mysql-applier*/CREATE TABLE IF NOT EXISTS %s.%s(
			id int(10) NOT NULL,
			binlog_file_name varchar(256) NOT NULL,
			position bigint(20) NOT NULL,
			kafka_topic varchar(1024) DEFAULT '' NOT NULL,
			kafka_offset bigint(20) DEFAULT -3 NOT NULL,
			lwm_offset bigint(20)  DEFAULT -3 NOT NULL,
			update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
		`, CheckpointDatabase, CheckpointTable)
	deleteTableSQL := fmt.Sprintf("DELETE FROM %s.%s;", CheckpointDatabase, CheckpointTable)
	initTableSQL := fmt.Sprintf("INSERT INTO %s.%s(id,binlog_file_name,position,"+
		"kafka_topic,kafka_offset,lwm_offset) VALUES(?,?,?,?,?,?)", CheckpointDatabase, CheckpointTable)

	//创建数据库
	_, err := o.db.Exec(createDBSQL)
	if err != nil {
		log.Errorf("Coordinator.InitCheckpointTable: exec create db error,err:%s,sql:%s", err.Error(), createDBSQL)
		return err
	}
	//创建表
	_, err = o.db.Exec(createTableSQL)
	if err != nil {
		log.Errorf("Coordinator.InitCheckpointTable: exec create table error,err:%s,sql:%s", err.Error(), createTableSQL)
		return err
	}
	//删除全部记录
	_, err = o.db.Exec(deleteTableSQL)
	if err != nil {
		log.Errorf("Coordinator.InitCheckpointTable: exec delete table error,err:%s,sql:%s", err.Error(), deleteTableSQL)
		return err
	}
	//插入1000条记录,设置默认值
	for i := 0; i < CheckpointTableCount; i++ {
		_, err = o.db.Exec(initTableSQL, i, "init_name", 0, o.config.KafkaTopic, o.config.KafkaOffset, o.config.KafkaOffset)
		if err != nil {
			log.Errorf("Coordinator.InitCheckpointTable: exec insert in table error,err:%s,sql:%s", err.Error(), initTableSQL)
			return err
		}
	}

	log.Infof("Coordinator.InitCheckpointTable: init checkpoint table successful,db:%s,table:%s",
		CheckpointDatabase, CheckpointTable)

	return nil
}

func (o *Coordinator) QuitNotify() <-chan struct{} {
	return o.quitC
}

// 获取sql_mode,并过滤NO_ZERO_DATE
func (o *Coordinator) getSqlMode() (string, error) {
	// 获取sql_mode
	sqlMode, err := utils.GetMysqlVar(o.config.MysqlHost, o.config.MysqlPort, o.config.MysqlUser, o.config.MysqlPasswd, "sql_mode")
	if err != nil {
		log.Errorf("Coordinator.Start: get mysql sql_mode error: %s", err)
		return "", err
	}

	// 关闭NO_ZERO_DATE
	sqlMode = strings.Replace(sqlMode, "NO_ZERO_DATE", "", -1)

	return sqlMode, nil
}

func (o *Coordinator) getServerId() (uint32, error) {
	tmp, err := utils.GetMysqlVar(o.config.MysqlHost, o.config.MysqlPort, o.config.MysqlUser, o.config.MysqlPasswd, "server_id")
	if err != nil {
		log.Errorf("Coordinator.Start: get mysql server_id error: %s", err)
		return 0, err
	}

	serverId, err := strconv.ParseUint(tmp, 10, 32)
	if err != nil {
		log.Errorf("Coordinator.Start: get mysql server_id error: %s", err)
		return 0, err
	}

	return uint32(serverId), nil
}

func (o *Coordinator) Start() error {
	var err error
	if o.started {
		return nil
	}
	defer func() {
		if err == nil {
			return
		}
		if o.db != nil {
			o.db.Close()
		}
	}()

	// 获取server_id
	o.serverId, err = o.getServerId()
	if err != nil {
		return err
	}
	log.Infof("Coordinator.Start: mysql server_id is: %d", o.serverId)

	// 获取sql_mode,并过滤NO_ZERO_DATE
	sqlMode, err := o.getSqlMode()
	if err != nil {
		return err
	}

	// 连接MySQL
	connectStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&writeTimeout=%ds&wait_timeout=%d",
		o.config.MysqlUser, o.config.MysqlPasswd, o.config.MysqlHost, o.config.MysqlPort,
		o.config.MysqlConnectTimeout, o.config.MysqlReadTimeout, o.config.MysqlWriteTimeout, o.config.MysqlWaitTimeout)
	connectStr += "&parseTime=true&loc=Asia%2FShanghai&interpolateParams=true"
	connectStr += fmt.Sprintf("&charset=%s", o.config.MysqlCharset)
	connectStr += fmt.Sprintf("&sql_mode='%s'", sqlMode)
	log.Infof("Coordinator.Start: mysql connect string: %s", connectStr)

	o.db, err = sql.Open("mysql", connectStr)
	if err != nil {
		log.Errorf("Coordinator.Start: open mysql error: %s", err)
		return err
	}

	o.db.SetMaxIdleConns(128)

	err = o.db.Ping()
	if err != nil {
		log.Errorf("Coordinator.Start: ping mysql error: %s", err)
		return err
	}

	// 初始化checkpoint表
	err = o.initCheckpointTable()
	if err != nil {
		log.Errorf("Coordinator.Start: initCheckpointTable error: %s", err)
		return err
	}

	// 注册status
	o.registerStatus()

	var ctx context.Context
	ctx, o.cancel = context.WithCancel(context.Background())
	go o.run(ctx)

	o.started = true

	return nil
}

func (o *Coordinator) Stop() error {
	if !o.started {
		return nil
	}

	// 通知协程退出
	o.cancel()
	// 等待协程退出
	<-o.quitC

	// 关闭数据库连接
	if o.db != nil {
		o.db.Close()
	}
	log.Warnf("Coordinator.Stop: stop succeed")
	o.started = false
	return nil
}

func (o *Coordinator) run(ctx context.Context) {
	// 通知外界已经退出
	defer close(o.quitC)

	t := time.NewTicker(300 * time.Millisecond)
	trxC := o.trxC
	ctxC := ctx.Done()

	needToQuit := false

	for {
		if o.getPendingTrx() != nil {
			trxC = nil
		} else {
			trxC = o.trxC
		}
		select {
		case trx := <-trxC:
			inChannelTrxCount.Update(int64(len(o.trxC)))
			// 新事务信号
			o.setPendingTrx(trx)
			log.Debugf("Coordinator.run: coordinating trx %s, offset: %d", trx.Gtid, trx.BeginOffset)

			o.tryScheduleTrx()

		case trxResult := <-o.trxResultC:
			// 协程退出信号
			o.runningTrxs--
			if o.runningTrxs == 0 && needToQuit {
				// 外界已经通知本协程退出
				return
			}
			trx := trxResult.Trx

			// 事务执行完毕，检查是否成功
			if trxResult.Err != nil {
				// 登记失败的事务信息，用于重试
				log.Errorf("Coordinator.run: failed trx %s, offset: %d,  %s", trx.Gtid, trx.BeginOffset, trxResult.Err)
				o.failedTrxs++
				o.addWaitToHandle(trx.Gtid, trxResult)
				break

			}

			trx.done = true
			log.Debugf("Coordinator.run: execed trx %s, offset: %d", trx.Gtid, trx.BeginOffset)

			// 统计成功执行的事务数量, tps
			trx.applyedTime = time.Now()
			applyedTrxCount.Inc(1)
			applyTps.Mark(1)

			// 统计总耗时、执行耗时
			if trx.RetryTimes == 0 {
				// 没失败过的事务才统计
				applyTime := int(trx.applyedTime.Sub(trx.scheduledTime) / time.Microsecond) // 微秒单位

				totalTimeHistogram.Update(int64(trx.applyedTime.Sub(trx.fetchedTime) / time.Millisecond)) //毫秒单位
				applyTimeHistogram.Update(int64(applyTime))

				// 如果执行时间超过一定阈值，记录到日志中
				if applyTime/1000 > o.config.SlowTrxThreshold {
					log.WithFields(log.Fields{
						"offset":          trx.BeginOffset,
						"size":            trx.EndOffset - trx.BeginOffset + 1,
						"gtid":            trx.Gtid,
						"binlog_position": fmt.Sprintf("%s:%d", trx.BinlogFileName, trx.Position),
						"apply_time":      applyTime / 1000,
					}).Warnf("Coordinator.run: found a slow trx")
				}
			}

			if o.getPendingTrx() != nil {
				o.caq.refreshLwm()
				o.tryScheduleTrx()
			}

		case <-t.C:
			// 对caq队列进行回收
			lastTrx := o.caq.moveQueueHead()
			if lastTrx != nil {
				// 更新succeedOffset
				o.setProgress(lastTrx)
			}
			// 更新并发度指标
			applyConcurrency.Update(int64(o.runningTrxs))

		case gtid := <-o.retryC:
			// 事务重试信号
			if trxResult, ok := o.waitToHandle[gtid]; ok {
				o.retryTrx(trxResult.Trx)
			}

		case gtid := <-o.skipC:
			// 事务跳过信号
			if trxResult, ok := o.waitToHandle[gtid]; ok {
				o.skipTrx(trxResult.Trx)

				if o.getPendingTrx() != nil {
					o.caq.refreshLwm()
					o.tryScheduleTrx()
				}
			}

		case <-ctxC:
			log.Infof("Coordinator.run: ctx canceled, need to quit")
			if o.runningTrxs != 0 {
				// 通知所有执行线程退出
				needToQuit = true
				o.cancelExecFunc()
				ctxC = nil
			} else {
				// 已经没有trx在执行
				return
			}
		}
	}
}
func (o *Coordinator) canSchedule() bool {
	trx := o.getPendingTrx()

	if o.runningTrxs == 0 && o.failedTrxs == 0 {
		return true
	}

	if trx.BinlogFileName != o.lastScheduledTrx.BinlogFileName {
		// binlog已经切换，需要等待所有事务执行完
		return false
	}

	if isSeqLeq(trx.LastCommitted, o.caq.estimateLwm()) {
		// 该事务LastCommitted小于等于低水位,可以调度
		return true
	}

	return false
}

func (o *Coordinator) tryScheduleTrx() {
	if !o.canSchedule() {
		return
	}

	opt := &ExecOpt{
		db:               o.db,
		allowDDL:         o.config.AllowDDL,
		strategyType:     StrategyType(o.config.HandleConflictStrategy),
		updateTimeColumn: o.config.UpdateTimeColumn,
		serverId:         o.serverId,
		resultC:          o.trxResultC,
	}
	trx := o.getPendingTrx()

	if o.lastScheduledTrx != nil && o.lastScheduledTrx.BinlogFileName != trx.BinlogFileName {
		// 遇到新的binlog，低水位需要重置为0
		o.caq.moveQueueHead()
		o.caq.resetLwm()
	}

	if o.lastScheduledTrx != nil &&
		trx.BeginOffset <= o.lastScheduledTrx.EndOffset {
		log.Fatal("Coordinator.scheduleTrx: trx offset less than max offset")
	}

	// 更新trx的LwmOffset字段
	lwmTrx := o.caq.estimateLwmTrx()
	if lwmTrx != nil {
		trx.LwmOffset = lwmTrx.BeginOffset
	} else {
		trx.LwmOffset = -3
	}

	o.runningTrxs++
	o.lastScheduledTrx = trx
	o.setPendingTrx(nil)

	if o.caq.enQueue(trx) == CAQ_UNDEF_INDEX {
		log.Panicf("Coordinator.scheduleTrx: caq is full")
	}

	// 执行该事务
	go o.execFunc(o.cancelExecCtx, opt, trx)

	// 统计调度耗时
	trx.scheduledTime = time.Now()
	scheduleTimeHistogram.Update(int64(trx.scheduledTime.Sub(trx.fetchedTime) / time.Millisecond))

	log.Debugf("Coordinator.scheduleTrx: coordinated trx %s, offset: %d", trx.Gtid, trx.BeginOffset)
}

func (o *Coordinator) retryTrx(trx *Trx) {
	opt := &ExecOpt{
		db:               o.db,
		allowDDL:         o.config.AllowDDL,
		strategyType:     StrategyType(o.config.HandleConflictStrategy),
		updateTimeColumn: o.config.UpdateTimeColumn,
		serverId:         o.serverId,
		resultC:          o.trxResultC,
	}
	o.delWaitToHandle(trx.Gtid)

	o.failedTrxs--
	o.runningTrxs++

	trx.RetryTimes++

	// 重试该事务
	go o.execFunc(o.cancelExecCtx, opt, trx)
	log.Warnf("Coordinator.retryTrx: coordinated trx %s", trx.Gtid)
}

func (o *Coordinator) skipTrx(trx *Trx) {
	trx.done = true
	o.delWaitToHandle(trx.Gtid)
	o.failedTrxs--
	log.Warnf("Coordinator.skipTrx: skipped trx %s", trx.Gtid)
}
