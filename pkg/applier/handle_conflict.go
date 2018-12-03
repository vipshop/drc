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
	"database/sql"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type StrategyType string

const (
	//基于最新时间优先策略，如果时间相等则利用Applier中的记录覆盖原记录
	TimeOverwriteStrategy = "time_overwrite"
	//基于最新时间优先策略，如果时间相等则忽略Applier中的记录
	TimeIgnoreStrategy = "time_ignore"
	IgnoreStrategy     = "ignore"
	OverwriteStrategy  = "overwrite"
)

func (s *ExecutableSQL) HandDeleteConflict(tx *sql.Tx, strategy StrategyType, updateTimeColumn string) error {
	//根据主键获取当前冲突的行记录
	currentRowNames, currentRowValues, err := s.getRowByPrimaryKey(tx)
	if err != nil && err != sql.ErrNoRows {
		log.Errorf("HandDeleteConflict:getRowByPrimaryKey error,err:%s", err.Error())
		return err
	}
	//打印忽略信息
	if len(currentRowValues) == 0 {
		log.WithFields(log.Fields{
			"file":                "handle_conflict.go",
			"func":                "HandDeleteConflict",
			"conflictType":        "deleteConflict",
			"trxGtid":             s.TrxGtid,
			"trxKafkaBeginOffset": s.TrxBeginOffset,
			"currentRowNames":     currentRowNames,
			"currentRow":          nil,
			"query":               s.Query,
			"applierRow":          s.Args,
			"updateTimeColumn":    updateTimeColumn,
		}).Warnf("HandDeleteConflict success,ignore applier row")
	} else {
		log.WithFields(log.Fields{
			"file":                "handle_conflict.go",
			"func":                "HandDeleteConflict",
			"conflictType":        "deleteConflict",
			"trxGtid":             s.TrxGtid,
			"trxKafkaBeginOffset": s.TrxBeginOffset,
			"query":               s.Query,
			"currentRowNames":     currentRowNames,
			"currentRow":          currentRowValues,
			"applierRow":          s.Args,
			"updateTimeColumn":    updateTimeColumn,
		}).Warnf("HandDeleteConflict success,ignore applier row")
	}

	return nil
}

func (s *ExecutableSQL) HandInsertConflict(tx *sql.Tx, strategy StrategyType, updateTimeColumn string) error {
	//根据主键获取当前冲突的行记录
	currentRowNames, currentRowValues, err := s.getRowByPrimaryKey(tx)
	if err != nil {
		log.Errorf("HandInsertConflict:getRowByPrimaryKey error,err:%s", err.Error())
		return err
	}

	switch strategy {
	case TimeOverwriteStrategy, TimeIgnoreStrategy:
		return s.handleInsertConflictByTime(tx, strategy, updateTimeColumn, currentRowNames, currentRowValues)
	case IgnoreStrategy:
		return s.handleInsertConflictByIgnore(tx, currentRowNames, currentRowValues)
	case OverwriteStrategy:
		return s.handleInsertConflictByOverwrite(tx, currentRowNames, currentRowValues)
	default:
		log.Errorf("HandInsertConflict:not support this strategy:%v", strategy)
		return ErrStrategyNotSupport
	}
	return nil
}

func (s *ExecutableSQL) HandleUpdateConflict(tx *sql.Tx, strategy StrategyType, updateTimeColumn string) error {
	//根据主键获取当前冲突的行记录
	currentRowNames, currentRowValues, err := s.getRowByPrimaryKey(tx)
	if err != nil {
		//主键不存在
		if err == sql.ErrNoRows {
			err := s.handleUpdatePkNotExist(tx)
			if err != nil {
				log.Errorf("HandleUpdateConflict:handleUpdatePkNotExist error,err:%s,s:%v", err.Error(), *s)
				return err
			}
			log.Warningf("HandleUpdateConflict:handleUpdatePkNotExist success,s:%v", *s)
			return nil
		}
		log.Errorf("HandInsertConflict:getRowByPrimaryKey error,err:%s,s=%v", err.Error(), *s)
		return err
	}
	log.Infof("HandleUpdateConflict:currentRowNames is %v,currentRowValues is %v", currentRowNames, currentRowValues)

	//根据不同策略做冲突处理
	switch strategy {
	case TimeOverwriteStrategy, TimeIgnoreStrategy:
		return s.handleUpdateConflictByTime(tx, strategy, updateTimeColumn, currentRowNames, currentRowValues)
	case IgnoreStrategy:
		return s.handleUpdateConflictByIgnore(tx, currentRowNames, currentRowValues)
	case OverwriteStrategy:
		return s.handleUpdateConflictByOverwrite(tx, currentRowNames, currentRowValues)
	default:
		log.Errorf("HandInsertConflict:not support this strategy:%v", strategy)
		return ErrStrategyNotSupport
	}
	return nil
}

//基于时间戳处理insert冲突
func (s *ExecutableSQL) handleInsertConflictByTime(tx *sql.Tx, strategy StrategyType, updateTimeColumn string,
	currentRowNames []string, currentRowValues []interface{}) error {
	var err error
	var ok bool
	var currentRowDrcTime time.Time
	var applierRowDrcTime time.Time

	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return err
	}

	for _, c := range s.RowValue.After {
		if EscapeName(c.Name) == updateTimeColumn {
			if applierRowDrcTime, ok = c.Value.(time.Time); !ok {
				log.Errorf("handleInsertConflictByTime:%s is not timestamp type,c:%v", updateTimeColumn, c)
				return ErrTimeType
			}
		}
	}

	for i, c := range currentRowNames {
		//在获取列名的时候，已经加上了反引号，这里不需要再次转义
		if c == updateTimeColumn {
			currentRowDrcTime, err = time.ParseInLocation(time.RFC3339, currentRowValues[i].(string), location)
			if err != nil {
				log.Errorf("handleInsertConflictByTime:ParseInLocation error,err:%s,currentRow[i]:%v",
					err.Error(), currentRowValues[i].(string))
				return err
			}
			break
		}
	}
	if currentRowDrcTime.IsZero() || applierRowDrcTime.IsZero() {
		log.Errorf("handleInsertConflictByTime:currentRowDrcTime[%v] or applierRowDrcTime[%v] is zero time,"+
			"updateTimeColumn:%s",
			currentRowDrcTime, applierRowDrcTime, updateTimeColumn)
		return ErrTimeIsZero
	}

	//根据时间戳优先原则，自动选择行记录
	if applierRowDrcTime.After(currentRowDrcTime) ||
		(applierRowDrcTime.Equal(currentRowDrcTime) && strategy == TimeOverwriteStrategy) {
		err = s.handleInsertConflictByOverwrite(tx, currentRowNames, currentRowValues)
		if err != nil {
			log.Errorf("handleInsertConflictByTime:err:%s,executableSQL:%v,currentRow:%v",
				err.Error(), *s, currentRowValues)
			return err
		}
	} else {
		err = s.handleInsertConflictByIgnore(tx, currentRowNames, currentRowValues)
		if err != nil {
			log.Errorf("handleInsertConflictByTime:err:%s,executableSQL:%v,currentRow:%v",
				err.Error(), *s, currentRowValues)
			return err
		}
	}

	return nil
}

//直接使用目标实例中的冲突记录，忽略当前记录
func (s *ExecutableSQL) handleInsertConflictByIgnore(tx *sql.Tx, currentRowNames []string, currentRowValues []interface{}) error {
	//获取列名
	columnNames := make([]string, 0, len(s.RowValue.After))
	for _, c := range s.RowValue.After {
		columnNames = append(columnNames, EscapeName(c.Name))
	}

	//打印忽略信息
	log.WithFields(log.Fields{
		"file":                "handle_conflict.go",
		"func":                "handleInsertConflictByIgnore",
		"conflictType":        "insertIgnore",
		"trxGtid":             s.TrxGtid,
		"trxKafkaBeginOffset": s.TrxBeginOffset,
		"query":               s.Query,
		"currentRowNames":     currentRowNames,
		"currentRow":          currentRowValues,
		"applierRowNames":     columnNames,
		"applierRowValues":    s.Args,
	}).Warnf("handleInsertConflictByIgnore success,ignore applier row")
	return nil
}

//使用当前记录覆盖目标实例中的冲突记录
func (s *ExecutableSQL) handleInsertConflictByOverwrite(tx *sql.Tx, currentRowNames []string, currentRowValues []interface{}) error {
	columnNames := make([]string, 0, len(s.RowValue.After))
	pkColumn := make([]*Column, 0, 2)
	pkColumnValues := make([]interface{}, 0, 2)

	args := make([]interface{}, 0, len(s.RowValue.After))
	for _, c := range s.RowValue.After {
		columnNames = append(columnNames, EscapeName(c.Name))
		if c.IsPk {
			pkColumn = append(pkColumn, c)
			pkColumnValues = append(pkColumnValues, c.Value)
		}
		args = append(args, c.Value)
	}

	//构造set条件
	setTokens := make([]string, 0, len(columnNames))
	for _, c := range columnNames {
		var setToken string
		setToken = fmt.Sprintf("%s=?", c)
		setTokens = append(setTokens, setToken)
	}
	setClause := strings.Join(setTokens, ", ")

	//构造where条件
	if len(pkColumn) == 0 || len(pkColumnValues) == 0 {
		return ErrNoPK
	}
	values := buildPreparedValues(len(pkColumn))
	equalsComparison, err := BuildEqualsComparison(pkColumn, values)
	if err != nil {
		return err
	}

	//构造冲突解决SQL,并执行
	query := fmt.Sprintf("update %s.%s set %s where %s",
		EscapeName(s.DatabaseName),
		EscapeName(s.TableName),
		setClause,
		equalsComparison,
	)

	//applierArgs + pk
	args = append(args, pkColumnValues...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		log.Errorf("handleInsertConflictByOverwrite:Exec query error,err:%s,query:%s,args:%+v",
			err.Error(), query, args)
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Errorf("handleInsertConflictByOverwrite:RowsAffected error,err:%s,query:%s,args:%+v",
			err.Error(), query, args)
		return err
	}

	switch count {
	case 0:
		log.Warnf("handleInsertConflictByOverwrite:the count of AffectedRows is 0,query:%s, args:%+v", query, args)
	case 1:
	default:
		log.Errorf("handleInsertConflictByOverwrite:the count of AffectedRows is not 1 or 0 ,count:%d,query:%s,args:%+v",
			count, query, args)
		return ErrorRowsAffectedCount
	}

	log.WithFields(log.Fields{
		"file":                "handle_conflict.go",
		"func":                "handleInsertConflictByOverwrite",
		"conflictType":        "insertOverwrite",
		"trxGtid":             s.TrxGtid,
		"trxKafkaBeginOffset": s.TrxBeginOffset,
		"query":               query,
		"args":                args,
		"currentRowNames":     currentRowNames,
		"currentRow":          currentRowValues,
		"applierRowNames":     columnNames,
		"Args":                s.Args,
	}).Warnf("handleInsertConflictByOverwrite success,overwrite current row")
	return nil
}

//基于时间戳处理update冲突
func (s *ExecutableSQL) handleUpdateConflictByTime(tx *sql.Tx, strategy StrategyType, updateTimeColumn string,
	currentRowNames []string, currentRowValues []interface{}) error {
	var err error
	var ok bool
	var currentRowDrcTime time.Time
	var applierRowDrcTime time.Time

	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return err
	}

	for _, c := range s.RowValue.After {
		if EscapeName(c.Name) == updateTimeColumn {
			if applierRowDrcTime, ok = c.Value.(time.Time); !ok {
				log.Errorf("handleUpdateConflictByTime:%s is not timestamp type,c:%v", updateTimeColumn, c)
				return ErrTimeType
			}
		}
	}

	for i, c := range currentRowNames {
		//在获取列名的时候，已经加上了反引号，这里不需要再次转义
		if c == updateTimeColumn {
			currentRowDrcTime, err = time.ParseInLocation(time.RFC3339, currentRowValues[i].(string), location)
			if err != nil {
				return err
			}
			break
		}
	}
	if currentRowDrcTime.IsZero() || applierRowDrcTime.IsZero() {
		log.Errorf("handleUpdateConflictByTime:currentRowDrcTime[%v] or applierRowDrcTime[%v] is zero time",
			currentRowDrcTime, applierRowDrcTime)
		return ErrTimeIsZero
	}

	//根据时间戳优先原则，自动选择行记录
	if applierRowDrcTime.After(currentRowDrcTime) ||
		(applierRowDrcTime.Equal(currentRowDrcTime) && strategy == TimeOverwriteStrategy) {
		err := s.handleUpdateConflictByOverwrite(tx, currentRowNames, currentRowValues)
		if err != nil {
			log.Errorf("handleUpdateConflictByTime:applier update time is after current time, "+
				"handleUpdateConflictByOverwrite error,err:%s,currentRow:%v,s=%v", err.Error(), currentRowValues, *s)
			return err
		}
	} else {
		err = s.handleUpdateConflictByIgnore(tx, currentRowNames, currentRowValues)
		if err != nil {
			log.Errorf("handleUpdateConflictByTime:current time is after applier update time , "+
				"handleUpdateConflictByIgnore error,err:%s,currentRow:%v,s=%v", err.Error(), currentRowValues, *s)
			return err
		}
	}

	return nil
}

//直接使用目标实例中的冲突记录，忽略当前记录
func (s *ExecutableSQL) handleUpdateConflictByIgnore(tx *sql.Tx, currentRowNames []string, currentRowValues []interface{}) error {
	beforeRowValues := make([]interface{}, 0, len(s.RowValue.Before))
	beforeRowNames := make([]string, 0, len(s.RowValue.Before))
	afterRowNames := make([]string, 0, len(s.RowValue.After))
	afterRowValues := make([]interface{}, 0, len(s.RowValue.After))

	for _, v := range s.RowValue.After {
		afterRowNames = append(afterRowNames, EscapeName(v.Name))
		afterRowValues = append(afterRowValues, v.Value)
	}

	for _, v := range s.RowValue.Before {
		beforeRowNames = append(beforeRowNames, EscapeName(v.Name))
		beforeRowValues = append(beforeRowValues, v.Value)
	}

	log.WithFields(log.Fields{
		"file":                "handle_conflict.go",
		"func":                "handleUpdateConflictByIgnore",
		"conflictType":        "updateIgnore",
		"trxGtid":             s.TrxGtid,
		"trxKafkaBeginOffset": s.TrxBeginOffset,
		"query":               s.Query,
		"currentRowNames":     currentRowNames,
		"currentRow":          currentRowValues,
		"beforeRowNames":      beforeRowNames,
		"beforeRowValues":     beforeRowValues,
		"afterRowNames":       afterRowNames,
		"afterRowValues":      afterRowValues,
	}).Warnf("handleUpdateConflictByIgnore success,ignore applier row")

	return nil
}

//使用当前记录覆盖目标实例中的冲突记录
func (s *ExecutableSQL) handleUpdateConflictByOverwrite(tx *sql.Tx, currentRowNames []string, currentRowValues []interface{}) error {
	beforeRowNames := make([]string, 0, len(s.RowValue.Before))
	beforeRowValues := make([]interface{}, 0, len(s.RowValue.Before))
	afterRowNames := make([]string, 0, len(s.RowValue.After))
	afterRowValues := make([]interface{}, 0, len(s.RowValue.After))
	pkColumn := make([]*Column, 0, 2)
	pkColumnValues := make([]interface{}, 0, 2)

	for _, v := range s.RowValue.After {
		afterRowNames = append(afterRowNames, EscapeName(v.Name))
		afterRowValues = append(afterRowValues, v.Value)
	}

	//获取前镜像的PK值
	for _, v := range s.RowValue.Before {
		beforeRowNames = append(beforeRowNames, EscapeName(v.Name))
		beforeRowValues = append(beforeRowValues, v.Value)
		if v.IsPk {
			pkColumn = append(pkColumn, v)
			pkColumnValues = append(pkColumnValues, v.Value)
		}
	}

	//构造set条件
	setTokens := make([]string, 0, len(afterRowNames))
	for _, c := range afterRowNames {
		var setToken string
		setToken = fmt.Sprintf("%s=?", c)
		setTokens = append(setTokens, setToken)
	}
	setClause := strings.Join(setTokens, ", ")

	//构造where条件
	values := buildPreparedValues(len(pkColumn))
	equalsComparison, err := BuildEqualsComparison(pkColumn, values)
	if err != nil {
		return err
	}

	//构造冲突解决SQL,并执行
	query := fmt.Sprintf("update %s.%s set %s where %s",
		EscapeName(s.DatabaseName),
		EscapeName(s.TableName),
		setClause,
		equalsComparison,
	)

	//构造参数列表:afterRow+pk
	afterRowValues = append(afterRowValues, pkColumnValues...)
	res, err := tx.Exec(query, afterRowValues...)
	if err != nil {
		log.Errorf("handleUpdateConflictByOverwrite:Exec query error,err:%s,query:%s,args:%+v",
			err.Error(), query, afterRowValues)
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Errorf("handleUpdateConflictByOverwrite:RowsAffected error,err:%s,query:%s,args:%+v",
			err.Error(), query, afterRowValues)
		return err
	}

	switch count {
	case 0:
		log.Warnf("handleUpdateConflictByOverwrite:the count of AffectedRows is 0,query:%s, args:%+v", query, afterRowValues)
	case 1:
	default:
		log.Errorf("handleUpdateConflictByOverwrite:the count of AffectedRows is not 1 or 0 ,count:%d,query:%s,args:%+v",
			count, query, afterRowValues)
		return ErrorRowsAffectedCount
	}

	log.WithFields(log.Fields{
		"file":                "handle_conflict.go",
		"func":                "handleUpdateConflictByOverwrite",
		"trxGtid":             s.TrxGtid,
		"trxKafkaBeginOffset": s.TrxBeginOffset,
		"query":               query,
		"afterRowValues":      afterRowValues,
		"conflictType":        "updateOverwrite",
		"currentRowNames":     currentRowNames,
		"currentRow":          currentRowValues,
		"beforeRowNames":      beforeRowNames,
		"beforeRowValues":     beforeRowValues,
		"afterRowNames":       afterRowNames,
	}).Warnf("handleUpdateConflictByOverwrite success,applier overwrite current row")

	return nil
}

//处理Update冲突时PK不存在的情况
func (s *ExecutableSQL) handleUpdatePkNotExist(tx *sql.Tx) error {
	beforeRowValues := make([]interface{}, 0, len(s.RowValue.Before))
	beforeRowNames := make([]string, 0, len(s.RowValue.Before))
	afterRowNames := make([]string, 0, len(s.RowValue.After))
	afterRowValues := make([]interface{}, 0, len(s.RowValue.After))

	for _, v := range s.RowValue.After {
		afterRowNames = append(afterRowNames, EscapeName(v.Name))
		afterRowValues = append(afterRowValues, v.Value)
	}
	for _, v := range s.RowValue.Before {
		beforeRowNames = append(beforeRowNames, EscapeName(v.Name))
		beforeRowValues = append(beforeRowValues, v.Value)
	}

	//构造列占位符
	values := buildPreparedValues(len(s.RowValue.After))
	preparedValues := strings.Join(values, ", ")

	//构造参数
	args := make([]interface{}, 0, len(s.RowValue.After))
	for _, c := range s.RowValue.After {
		args = append(args, c.Value)
	}
	query := fmt.Sprintf("insert into %s.%s(%s) values(%s)",
		EscapeName(s.DatabaseName),
		EscapeName(s.TableName),
		strings.Join(afterRowNames, ", "),
		preparedValues,
	)
	_, err := tx.Exec(query, args...)
	if err != nil {
		log.Errorf("handleUpdatePkNotExist,err:%s,query:%s,args:%v", err.Error(), query, args)
		return err
	}

	log.WithFields(log.Fields{
		"file":                "handle_conflict.go",
		"func":                "handleUpdateConflictByOverwrite",
		"trxGtid":             s.TrxGtid,
		"trxKafkaBeginOffset": s.TrxBeginOffset,
		"query":               query,
		"args":                args,
		"conflictType":        "updateNoPk",
		"beforeRowNames":      beforeRowNames,
		"beforeRowValues":     beforeRowValues,
		"afterRowNames":       afterRowNames,
		"afterRowValues":      afterRowValues,
	}).Warnf("handleUpdatePkNotExist success,applier insert after row")

	return nil
}

//返回值：columnName,columnValue,error
func (s *ExecutableSQL) getRowByPrimaryKey(tx *sql.Tx) ([]string, []interface{}, error) {
	var currentRow []interface{}
	//获取列名称
	columnNames := make([]string, 0, 8)
	pkColumn := make([]*Column, 0, 2)
	pkColumnValues := make([]interface{}, 0, 2)
	switch s.EventType {
	case UPDATE_ROWS_EVENT, DELETE_ROWS_EVENT:
		for _, c := range s.RowValue.Before {
			columnNames = append(columnNames, EscapeName(c.Name))
			if c.IsPk {
				pkColumn = append(pkColumn, c)
				pkColumnValues = append(pkColumnValues, c.Value)
			}
		}
		currentRow = make([]interface{}, len(s.RowValue.Before))
	case WRITE_ROWS_EVENT:
		for _, c := range s.RowValue.After {
			columnNames = append(columnNames, EscapeName(c.Name))
			if c.IsPk {
				pkColumn = append(pkColumn, c)
				pkColumnValues = append(pkColumnValues, c.Value)
			}
		}
		currentRow = make([]interface{}, len(s.RowValue.After))
	}

	if len(pkColumn) == 0 {
		log.Errorf("getRowByPrimaryKey:no PK,s:%v", *s)
		return nil, nil, ErrNoPK
	}
	//构造where条件
	values := buildPreparedValues(len(pkColumn))
	equalsComparison, err := BuildEqualsComparison(pkColumn, values)
	if err != nil {
		return nil, nil, err
	}
	selectColumns := strings.Join(columnNames, ", ")
	query := fmt.Sprintf("select %s from %s.%s where %s",
		selectColumns,
		EscapeName(s.DatabaseName),
		EscapeName(s.TableName),
		equalsComparison,
	)

	//获取列值
	rowValues := make([]sql.NullString, len(currentRow))
	for i := range rowValues {
		currentRow[i] = &rowValues[i]
	}
	err = tx.QueryRow(query, pkColumnValues...).Scan(currentRow...)
	if err != nil {
		var conflictType string
		switch s.EventType {
		case UPDATE_ROWS_EVENT:
			conflictType = "HandleUpdateConflict"
		case WRITE_ROWS_EVENT:
			conflictType = "HandInsertConflict"
		case DELETE_ROWS_EVENT:
			conflictType = "HandDeleteConflict"
		}

		log.Warnf("%s:getRowByPrimaryKey:Scan Row error,err: %s, query:%s, pk:%v", conflictType, err, query, pkColumnValues)
		return nil, nil, err
	}

	//将列值转换成string
	result := make([]interface{}, 0, len(currentRow))
	for _, v := range rowValues {
		if v.Valid {
			result = append(result, v.String)
		} else {
			result = append(result, "NULL")
		}

	}
	return columnNames, result, nil
}
