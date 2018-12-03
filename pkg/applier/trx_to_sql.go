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
	"fmt"
	"time"

	"strings"

	log "github.com/Sirupsen/logrus"
	"strconv"
)

type EventType uint32
type ValueComparisonSign string

const (
	QUERY_EVENT       EventType = 2
	WRITE_ROWS_EVENT            = 30
	UPDATE_ROWS_EVENT           = 31
	DELETE_ROWS_EVENT           = 32
	GTID_LOG_EVENT              = 33
)

const (
	EqualsComparisonSign     ValueComparisonSign = "="
	NullEqualsComparisonSign ValueComparisonSign = "is"
)

// Column
type Column struct {
	// 名称
	Name string
	// int, varchar, blob等
	Type       string
	BinlogType ColumnType
	// 内容
	Value interface{}
	// 是否为pk
	IsPk bool
	// 是否为NULL
	IsNull bool
}

type Row struct {
	// Delete情况下，只有before
	Before []*Column
	// Insert情况下，只有after
	// Update情况下，before和after成对出现
	After []*Column
}

type Event struct {
	DatabaseName string
	TableName    string
	EventType    EventType
	// 数据库的binlog时间
	Timestamp uint64
	// RDP处理binlog的时间
	TimestampOfReceipt uint64
	// 在binlog文件中的位置
	Position uint64
	// 下一条binlog的位置
	NextPosition uint64
	// binlog文件名
	BinlogFileName string
	ServerId       uint64
	// Query对应的SQL语句
	SqlStatement string
	Rows         []*Row
}

// Transaction 数据
type Trx struct {
	// kafka topic
	Topic string `json:"topic"`
	// kafka offset
	BeginOffset int64 `json:"begin_offset"`
	EndOffset   int64 `json:"end_offset"`
	// LwmOffset 表示该kafka offset之前(含)的事务都已经执行过
	LwmOffset int64 `json:"lwm_offset"`

	Gtid string `json:"gtid"`
	// 一个Gtid范围内，有过多的event时，导致拆包的序列号，从0、1、2...
	Seq uint64 `json:"-"`
	// 在binlog文件中的位置
	Position uint64 `json:"position"`
	// binlog文件名
	BinlogFileName string `json:"binlog_file_name"`
	// 下一条binlog的位置
	NextPosition uint64 `json:"-"`
	// binlog文件名
	NextBinlogFileName string `json:"-"`
	// 组提交id
	LastCommitted  int64 `json:"last_committed"`
	SequenceNumber int64 `json:"sequence_number"`

	// 事务产生的时间，取自GTID
	Timestamp uint64 `json:"timestamp"`

	// 重试次数
	RetryTimes uint32 `json:"retry_times"`

	Events []*Event `json:"events"`

	fetchedTime   time.Time
	scheduledTime time.Time
	applyedTime   time.Time

	// 标记是否执行完成
	done bool
}

type ExecutableSQL struct {
	Query string
	Args  []interface{}

	TrxGtid        string
	TrxBeginOffset int64

	EventType    EventType
	DatabaseName string
	TableName    string
	RowValue     *Row
}

func (tx *Trx) BuildTransactionSQL() ([]*ExecutableSQL, error) {
	//循环解析事务中的Event并生成对应的SQL
	totalSQLs := make([]*ExecutableSQL, 0, 16)
	for _, event := range tx.Events {
		//大事务被过滤掉了
		//需要告警
		if len(event.Rows) == 0 && (event.EventType == DELETE_ROWS_EVENT || event.EventType == UPDATE_ROWS_EVENT ||
			event.EventType == WRITE_ROWS_EVENT) {
			log.Errorf("BuildTransactionSQL:large trx has been filtered by rdp. eventType:%v,binglogFile:%s,position:%d",
				event.EventType, event.BinlogFileName, event.Position)
			return nil, ErrLargeTrx
		}

		switch event.EventType {
		case DELETE_ROWS_EVENT:
			//构造delete SQL
			deleteSQLs, err := event.BuildDeleteSQL(tx.Gtid, tx.BeginOffset)
			if err != nil {
				log.Errorf("BuildTransactionSQL: BuildDeleteSQL error,err:%s,event:%v",
					err, *event)
				return nil, err
			}
			totalSQLs = append(totalSQLs, deleteSQLs...)
		case UPDATE_ROWS_EVENT:
			//构造update SQL
			updateSQLs, err := event.BuildUpdateSQL(tx.Gtid, tx.BeginOffset)
			if err != nil {
				log.Errorf("BuildTransactionSQL:BuildUpdateSQL error,err:%s,event:%v",
					err, *event)
				return nil, err
			}
			totalSQLs = append(totalSQLs, updateSQLs...)
		case WRITE_ROWS_EVENT:
			//构造insert SQL
			insertSQLs, err := event.BuildInsertSQL(tx.Gtid, tx.BeginOffset)
			if err != nil {
				log.Errorf("BuildTransactionSQL:BuildInsertSQL error,err:%s,event:%v",
					err, *event)
				return nil, err
			}
			totalSQLs = append(totalSQLs, insertSQLs...)
		case QUERY_EVENT:
			//DDL
			if len(event.SqlStatement) != 0 {
				//增加hint
				query := fmt.Sprintf("/*mysql-applier*/%s", event.SqlStatement)
				executableSQL := &ExecutableSQL{
					Query:          query,
					EventType:      QUERY_EVENT,
					DatabaseName:   event.DatabaseName,
					TableName:      event.TableName,
					Args:           nil,
					RowValue:       nil,
					TrxGtid:        tx.Gtid,
					TrxBeginOffset: tx.BeginOffset,
				}
				totalSQLs = append(totalSQLs, executableSQL)
			} else {
				log.Fatalf("BuildTransactionSQL: event type is QUERY_EVENT, but SqlStatement is nil,event:%v", *event)
			}
		default:
			log.Fatalf("BuildTransactionSQL:Event:%v", *event)
		}
	}
	return totalSQLs, nil
}

func (e *Event) BuildDeleteSQL(gtid string, beginOffset int64) ([]*ExecutableSQL, error) {
	if e.EventType != DELETE_ROWS_EVENT {
		return nil, ErrNotDeleteType
	}
	executableSQLs := make([]*ExecutableSQL, 0, 8)

	//构造SQL
	databaseName := EscapeName(e.DatabaseName)
	tableName := EscapeName(e.TableName)
	if len(databaseName) == 0 || len(tableName) == 0 {
		log.Errorf("BuildDeleteSQL:args error,databaseName:%s,tableName:%s,equalsComparison:%s",
			databaseName, tableName)
		return nil, ErrArgsIsNil
	}

	//构造参数
	for i, r := range e.Rows {
		//构造where条件
		equalsComparison, err := e.BuildEqualsPreparedComparison(i)
		if err != nil {
			log.Errorf("BuildDeleteSQL:BuildEqualsPreparedComparison error,err:%s,event:%v",
				err, *e)
			return nil, err
		}
		if len(equalsComparison) == 0 {
			log.Errorf("BuildDeleteSQL:len(equalsComparison) is 0")
			return nil, ErrArgsIsNil
		}

		query := fmt.Sprintf("delete from %s.%s where %s",
			databaseName,
			tableName,
			equalsComparison,
		)

		if len(r.Before) == 0 {
			log.Errorf("BuildDeleteSQL:r.Before is nil,event:%v", *e)
			return nil, ErrRowArrayIsNil
		}
		//不存在PK
		if exist := HasPk(r.Before); !exist {
			return nil, ErrNoPK
		}
		args, err := buildArgsValue(r.Before, true)
		if err != nil {
			return nil, err
		}
		executableSQL := &ExecutableSQL{
			Query:          query,
			Args:           args,
			EventType:      DELETE_ROWS_EVENT,
			DatabaseName:   databaseName,
			TableName:      tableName,
			RowValue:       r,
			TrxGtid:        gtid,
			TrxBeginOffset: beginOffset,
		}
		executableSQLs = append(executableSQLs, executableSQL)
	}

	return executableSQLs, nil
}

func (e *Event) BuildUpdateSQL(gtid string, beginOffset int64) ([]*ExecutableSQL, error) {
	if e.EventType != UPDATE_ROWS_EVENT {
		return nil, ErrNotUpdateType
	}
	executableSQLs := make([]*ExecutableSQL, 0, 8)

	//构造set条件
	setClause, err := e.BuildSetPreparedClause()
	if err != nil {
		log.Errorf("BuildUpdateSQL:BuildSetPreparedClause error,err:%s,event:%v",
			err, *e)
		return nil, err
	}

	//构造DB和table
	databaseName := EscapeName(e.DatabaseName)
	tableName := EscapeName(e.TableName)
	if len(databaseName) == 0 || len(tableName) == 0 ||
		len(setClause) == 0 {
		log.Errorf("BuildUpdateSQL:args error,databaseName:%s,tableName:%s,setClause:%s",
			databaseName, tableName, setClause)
		return nil, ErrArgsIsNil
	}

	//构造query和对应的参数
	for i, r := range e.Rows {
		//构造where条件
		equalsComparison, err := e.BuildEqualsPreparedComparison(i)
		if err != nil {
			log.Errorf("BuildUpdateSQL:BuildEqualsPreparedComparison error,err:%s,event:%v",
				err, *e)
			return nil, err
		}
		if len(equalsComparison) == 0 {
			log.Errorf("BuildUpdateSQL:len(equalsComparison) is 0")
			return nil, ErrArgsIsNil
		}
		//构造query
		query := fmt.Sprintf("update %s.%s set %s where %s",
			databaseName,
			tableName,
			setClause,
			equalsComparison,
		)

		if len(r.Before) == 0 || len(r.After) == 0 {
			log.Errorf("BuildUpdateSQL:r.Before or After is nil,event:%v", *e)
			return nil, ErrRowArrayIsNil
		}
		//不存在PK
		if exist := HasPk(r.Before); !exist {
			return nil, ErrNoPK
		}
		afterArgs, err := buildArgsValue(r.After, false)
		if err != nil {
			return nil, err
		}
		beforeArgs, err := buildArgsValue(r.Before, true)
		if err != nil {
			return nil, err
		}
		afterArgs = append(afterArgs, beforeArgs...)
		executableSQL := &ExecutableSQL{
			Query:          query,
			Args:           afterArgs,
			EventType:      UPDATE_ROWS_EVENT,
			DatabaseName:   databaseName,
			TableName:      tableName,
			RowValue:       r,
			TrxGtid:        gtid,
			TrxBeginOffset: beginOffset,
		}
		executableSQLs = append(executableSQLs, executableSQL)
	}
	return executableSQLs, nil
}

func (e *Event) BuildInsertSQL(gtid string, beginOffset int64) ([]*ExecutableSQL, error) {
	if e.EventType != WRITE_ROWS_EVENT {
		return nil, ErrNotInsertType
	}
	executableSQLs := make([]*ExecutableSQL, 0, 8)

	//构造列名称
	if len(e.Rows[0].After) == 0 {
		log.Errorf("BuildInsertSQL:r.After is nil,event:%v", *e)
		return nil, ErrColumnArrayIsNil
	}
	columnNames := make([]string, 0, len(e.Rows[0].After))
	for _, c := range e.Rows[0].After {
		columnNames = append(columnNames, EscapeName(c.Name))
	}
	//构造列占位符
	preparedValues, err := e.BuildColumnsPreparedValues()
	if err != nil {
		log.Errorf("BuildInsertSQL:BuildColumnsPreparedValues error,err:%s,event:%v",
			err, *e)
		return nil, err
	}

	//构造SQL
	databaseName := EscapeName(e.DatabaseName)
	tableName := EscapeName(e.TableName)
	if len(databaseName) == 0 || len(tableName) == 0 ||
		len(preparedValues) == 0 {
		log.Errorf("BuildInsertSQL:args error,databaseName:%s,tableName:%s,preparedValues:%s",
			databaseName, tableName, preparedValues)
		return nil, ErrArgsIsNil
	}

	query := fmt.Sprintf("insert into %s.%s(%s) values(%s)",
		databaseName,
		tableName,
		strings.Join(columnNames, ", "),
		preparedValues,
	)

	//构造参数
	for _, r := range e.Rows {
		if len(r.After) == 0 {
			log.Errorf("BuildInsertSQL:r.After is nil,event:%v", *e)
			return nil, err
		}
		//不存在PK
		if exist := HasPk(r.After); !exist {
			return nil, ErrNoPK
		}
		args, err := buildArgsValue(r.After, false)
		if err != nil {
			return nil, err
		}
		executableSQL := &ExecutableSQL{
			Query:          query,
			Args:           args,
			EventType:      WRITE_ROWS_EVENT,
			DatabaseName:   databaseName,
			TableName:      tableName,
			RowValue:       r,
			TrxGtid:        gtid,
			TrxBeginOffset: beginOffset,
		}
		executableSQLs = append(executableSQLs, executableSQL)
	}
	return executableSQLs, nil
}

//构造insert中列信息
func (e *Event) BuildColumnsPreparedValues() (string, error) {
	if len(e.Rows[0].After) == 0 {
		return "", ErrColumnArrayIsNil
	}
	values := buildPreparedValues(len(e.Rows[0].After))
	return strings.Join(values, ", "), nil
}

//构造where条件
func (e *Event) BuildEqualsPreparedComparison(i int) (string, error) {
	if len(e.Rows[i].Before) == 0 {
		return "", ErrColumnArrayIsNil
	}
	//获取列名称
	column := make([]*Column, 0, len(e.Rows[i].Before))
	for _, c := range e.Rows[i].Before {
		//浮点型,binary类型不作为where条件
		if c.BinlogType == MYSQL_TYPE_DOUBLE || c.BinlogType == MYSQL_TYPE_FLOAT ||
			(c.BinlogType == MYSQL_TYPE_STRING && strings.HasPrefix(c.Type, "binary")) {
			continue
		}
		column = append(column, c)
	}

	values := buildPreparedValues(len(column))
	return BuildEqualsComparison(column, values)
}

func BuildEqualsComparison(columns []*Column, values []string) (string, error) {
	var equalComparisonSign ValueComparisonSign
	if len(columns) == 0 {
		return "", ErrColumnArrayIsNil
	}
	if len(columns) != len(values) {
		return "", ErrArrayLengthNotEqual
	}
	comparisons := make([]string, 0, len(columns))
	for i, column := range columns {
		value := values[i]
		if column.IsNull {
			equalComparisonSign = NullEqualsComparisonSign
		} else {
			equalComparisonSign = EqualsComparisonSign
		}
		comparison, err := BuildValueComparison(column.Name, value, equalComparisonSign)
		if err != nil {
			return "", err
		}
		comparisons = append(comparisons, comparison)
	}
	result := strings.Join(comparisons, " and ")
	result = fmt.Sprintf("(%s)", result)
	return result, nil
}

func buildPreparedValues(length int) []string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return values
}

func BuildValueComparison(column string, value string, comparisonSign ValueComparisonSign) (string, error) {
	if column == "" {
		return "", ErrColumnIsNil
	}
	if value == "" {
		return "", ErrValueIsNil
	}
	comparison := fmt.Sprintf("(%s %s %s)", EscapeName(column), string(comparisonSign), value)
	return comparison, nil
}

//构造update中set子句
func (e *Event) BuildSetPreparedClause() (string, error) {
	if len(e.Rows) == 0 {
		return "", ErrRowArrayIsNil
	}
	if len(e.Rows[0].After) == 0 {
		return "", ErrRowArrayIsNil
	}
	setTokens := make([]string, 0, len(e.Rows[0].After))
	for _, c := range e.Rows[0].After {
		var setToken string
		setToken = fmt.Sprintf("%s=?", EscapeName(c.Name))
		setTokens = append(setTokens, setToken)
	}
	return strings.Join(setTokens, ", "), nil
}

func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

//构造参数列表
func buildArgsValue(columns []*Column, filter bool) ([]interface{}, error) {
	args := make([]interface{}, 0, len(columns))
	for _, c := range columns {
		//where条件需要过滤浮点型和binary
		if filter {
			if c.BinlogType == MYSQL_TYPE_DOUBLE || c.BinlogType == MYSQL_TYPE_FLOAT ||
				(c.BinlogType == MYSQL_TYPE_STRING && strings.HasPrefix(c.Type, "binary")) {
				continue
			}
		}
		args = append(args, c.Value)
	}
	return args, nil
}

func HasPk(columns []*Column) bool {
	for _, c := range columns {
		if c.IsPk {
			return true
		}
	}
	return false
}
