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
	"testing"

	"log"

	"github.com/stretchr/testify/assert"
)

const (
	TestDatabase      = "drc_test_database"
	TestTable         = "drc_test_table"
	DrcTimeColumnName = "`drc_update_time`"
)

func TestBuildTransactionSQL(t *testing.T) {
	{
		//update+delete+insert
		trx := newMockTrx()
		executableSQLs, err := trx.BuildTransactionSQL()
		assert.Nil(t, err)
		assert.Equal(t, len(executableSQLs), 3)

		//update
		assert.Equal(t, executableSQLs[0].Query,
			"update `drc_test_database`.`drc_test_table` set `c1`=?, `c2`=?, `c3`=? where ((`c1` = ?) and (`c2` = ?) and (`c3` = ?))")
		assert.Equal(t, executableSQLs[0].Args, []interface{}{"21", "334", "def", "1", "34", "abc"})
		assert.Equal(t, executableSQLs[0].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQLs[0].TableName, EscapeName(TestTable))

		//delete
		assert.Equal(t, executableSQLs[1].Query,
			"delete from `drc_test_database`.`drc_test_table` where ((`c1` = ?) and (`c2` = ?) and (`c3` = ?))")
		assert.Equal(t, executableSQLs[1].Args, []interface{}{"67", "39", "hello"})
		assert.Equal(t, executableSQLs[1].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQLs[1].TableName, EscapeName(TestTable))

		//insert
		assert.Equal(t, executableSQLs[2].Query,
			"insert into `drc_test_database`.`drc_test_table`(`c1`, `c2`, `c3`) values(?, ?, ?)")
		assert.Equal(t, executableSQLs[2].Args, []interface{}{"90", "312", "world"})
		assert.Equal(t, executableSQLs[2].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQLs[2].TableName, EscapeName(TestTable))
	}

}

func TestBuildDeleteSQL(t *testing.T) {
	{
		deleteEvent := newDeleteEvent([]string{"c1"}, []string{"c1"}, []string{"1"})
		executableSQL, err := deleteEvent.BuildDeleteSQL("13628a82-b49a-11e8-a37c-c88d834bfdaf:3", 1234)
		assert.Nil(t, err)

		assert.Equal(t, len(executableSQL), 1)
		assert.Equal(t, executableSQL[0].Query, "delete from `drc_test_database`.`drc_test_table` where ((`c1` = ?))")
		assert.Equal(t, executableSQL[0].Args, []interface{}{"1"})
		assert.Equal(t, executableSQL[0].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQL[0].TableName, EscapeName(TestTable))
		assert.Equal(t, executableSQL[0].RowValue, deleteEvent.Rows[0])
	}

	{
		deleteEvent := newDeleteEvent([]string{"c1", "c2", "c3"}, []string{"c1"}, []string{"1", "2", "3"})
		executableSQL, err := deleteEvent.BuildDeleteSQL("13628a82-b49a-11e8-a37c-c88d834bfdaf:4", 1235)
		assert.Nil(t, err)

		assert.Equal(t, len(executableSQL), 1)
		assert.Equal(t, executableSQL[0].Query,
			"delete from `drc_test_database`.`drc_test_table` where ((`c1` = ?) and (`c2` = ?) and (`c3` = ?))")
		assert.Equal(t, executableSQL[0].Args, []interface{}{"1", "2", "3"})
		assert.Equal(t, executableSQL[0].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQL[0].TableName, EscapeName(TestTable))
		assert.Equal(t, executableSQL[0].RowValue, deleteEvent.Rows[0])
	}
}

func TestBuildUpdateSQL(t *testing.T) {
	{
		updateEvent := newUpdateEvent([]string{"c1", "c2", "c3"},
			[]string{"c1"}, []string{"1", "2", "ab"}, []string{"10", "34", "de"})
		executableSQL, err := updateEvent.BuildUpdateSQL("13628a82-b49a-11e8-a37c-c88d834bfdaf:5", 1236)
		assert.Nil(t, err)

		assert.Equal(t, len(executableSQL), 1)
		assert.Equal(t, executableSQL[0].Query,
			"update `drc_test_database`.`drc_test_table` set `c1`=?, `c2`=?, `c3`=? where ((`c1` = ?) and (`c2` = ?) and (`c3` = ?))")
		assert.Equal(t, executableSQL[0].Args, []interface{}{"10", "34", "de", "1", "2", "ab"})
		assert.Equal(t, executableSQL[0].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQL[0].TableName, EscapeName(TestTable))
		assert.Equal(t, executableSQL[0].RowValue, updateEvent.Rows[0])
	}
}

func TestBuildInsertSQL(t *testing.T) {
	{
		insertEvent := newInsertEvent([]string{"c1", "c2", "c3"},
			[]string{"c1"}, []string{"1", "2", "ab"})
		executableSQL, err := insertEvent.BuildInsertSQL("13628a82-b49a-11e8-a37c-c88d834bfdaf:6", 1237)
		assert.Nil(t, err)

		assert.Equal(t, len(executableSQL), 1)
		assert.Equal(t, executableSQL[0].Query,
			"insert into `drc_test_database`.`drc_test_table`(`c1`, `c2`, `c3`) values(?, ?, ?)")
		assert.Equal(t, executableSQL[0].Args, []interface{}{"1", "2", "ab"})
		assert.Equal(t, executableSQL[0].DatabaseName, EscapeName(TestDatabase))
		assert.Equal(t, executableSQL[0].TableName, EscapeName(TestTable))
		assert.Equal(t, executableSQL[0].RowValue, insertEvent.Rows[0])
	}
}

func TestBuildColumnsPreparedValues(t *testing.T) {
	{
		insertEvent := newInsertEvent([]string{"c1", "c2", "c3"},
			[]string{"c1"}, []string{"1", "2", "ab"})
		values, err := insertEvent.BuildColumnsPreparedValues()
		assert.Nil(t, err)
		assert.Equal(t, values, "?, ?, ?")

	}
}

func TestBuildEqualsPreparedComparison(t *testing.T) {
	{
		updateEvent := newUpdateEvent([]string{"c1", "c2"}, []string{"c1"}, []string{"1", "3"}, []string{"2", "4"})
		comparison, err := updateEvent.BuildEqualsPreparedComparison(0)
		assert.Nil(t, err)
		assert.Equal(t, comparison, "((`c1` = ?) and (`c2` = ?))")
	}
}

func TestBuildEqualsComparison(t *testing.T) {
	{
		column1 := &Column{
			Name:  "c1",
			Value: "@v1",
		}
		values := []string{"@v1"}
		comparison, err := BuildEqualsComparison([]*Column{column1}, values)
		assert.Nil(t, err)
		assert.Equal(t, comparison, "((`c1` = @v1))")
	}
	{
		column1 := &Column{
			Name:  "c1",
			Value: "@v1",
		}
		column2 := &Column{
			Name:  "c2",
			Value: "@v2",
		}
		values := []string{"@v1", "@v2"}
		comparison, err := BuildEqualsComparison([]*Column{column1, column2}, values)
		assert.Nil(t, err)
		assert.Equal(t, comparison, "((`c1` = @v1) and (`c2` = @v2))")
	}
	{
		column1 := &Column{
			Name:   "c1",
			Value:  "NULL",
			IsNull: true,
		}
		column2 := &Column{
			Name:  "c2",
			Value: "@v2",
		}
		values := []string{"NULL", "@v2"}
		comparison, err := BuildEqualsComparison([]*Column{column1, column2}, values)
		assert.Nil(t, err)
		assert.Equal(t, comparison, "((`c1` is NULL) and (`c2` = @v2))")
	}

	{
		column1 := &Column{
			Name:  "c1",
			Value: "@v1",
		}
		values := []string{"@v1", "@v2"}
		_, err := BuildEqualsComparison([]*Column{column1}, values)
		assert.NotNil(t, err)
	}
	{
		values := make([]string, 0, 1)
		_, err := BuildEqualsComparison([]*Column{}, values)
		assert.NotNil(t, err)
	}

}

func TestBuildValueComparison(t *testing.T) {
	{
		columns := []string{"c1", "c2"}
		values := []string{"24", "abde"}
		comparisons := make([]string, 0, 2)
		for i, column := range columns {
			comparison, err := BuildValueComparison(column, values[i], "=")
			assert.Nil(t, err)
			comparisons = append(comparisons, comparison)
		}
		assert.Equal(t, comparisons, []string{"(`c1` = 24)", "(`c2` = abde)"})
	}
}

//测试update set条件
func TestBuildSetPreparedClause(t *testing.T) {
	{
		updateEvent := newUpdateEvent([]string{"c1"}, []string{"c1"}, []string{"1"}, []string{"2"})
		clause, err := updateEvent.BuildSetPreparedClause()
		assert.Nil(t, err)
		assert.Equal(t, clause, "`c1`=?")
	}
	{
		updateEvent := newUpdateEvent([]string{"c1", "c2"}, []string{"c1"}, []string{"1", "3"}, []string{"2", "4"})
		clause, err := updateEvent.BuildSetPreparedClause()
		assert.Nil(t, err)
		assert.Equal(t, clause, "`c1`=?, `c2`=?")
	}
	{
		updateEvent := newUpdateEvent([]string{}, nil, []string{}, []string{})
		_, err := updateEvent.BuildSetPreparedClause()
		assert.NotNil(t, err)
	}
}

func newMockTrx() *Trx {
	updateEvent := newUpdateEvent([]string{"c1", "c2", "c3"}, []string{"c1"}, []string{"1", "34", "abc"}, []string{"21", "334", "def"})
	deleteEvent := newDeleteEvent([]string{"c1", "c2", "c3"}, []string{"c1"}, []string{"67", "39", "hello"})
	insertEvent := newInsertEvent([]string{"c1", "c2", "c3"}, []string{"c1"}, []string{"90", "312", "world"})
	return &Trx{
		Events: []*Event{updateEvent, deleteEvent, insertEvent},
	}
}

func newUpdateEvent(columnName []string, pkNames []string, beforeValues, afterValues []string) *Event {
	row := newBeforeAndAfterRow(columnName, pkNames, beforeValues, afterValues)

	return &Event{
		DatabaseName: TestDatabase,
		TableName:    TestTable,
		EventType:    UPDATE_ROWS_EVENT,
		Rows:         []*Row{row},
	}
}

func newDeleteEvent(columnName []string, pkNames []string, beforeValues []string) *Event {
	row := newBeforeRow(columnName, pkNames, beforeValues)

	return &Event{
		DatabaseName: TestDatabase,
		TableName:    TestTable,
		EventType:    DELETE_ROWS_EVENT,
		Rows:         []*Row{row},
	}
}

func newInsertEvent(columnName []string, pkNames []string, afterValues []string) *Event {
	row := newAfterRow(columnName, pkNames, afterValues)

	return &Event{
		DatabaseName: TestDatabase,
		TableName:    TestTable,
		EventType:    WRITE_ROWS_EVENT,
		Rows:         []*Row{row},
	}
}

func newQueryEvent(sql string) *Event {
	return &Event{
		DatabaseName: TestDatabase,
		TableName:    TestTable,
		EventType:    QUERY_EVENT,
		Rows:         nil,
		SqlStatement: sql,
	}
}

//创建只包含Before的Row
func newBeforeRow(columnName []string, pkNames []string, values []string) *Row {
	if len(columnName) != len(values) {
		log.Fatalf("newBeforeRow:len(columnName)=%d,len(values)=%d,",
			len(columnName), len(values))
	}

	columns := make([]*Column, 0, 16)
	for i, name := range columnName {
		c := &Column{
			Name: name,
		}
		for j := 0; j < len(pkNames); j++ {
			if pkNames[j] == name {
				c.IsPk = true
				break
			}
		}
		if EscapeName(name) == DrcTimeColumnName {
			c.BinlogType = MYSQL_TYPE_TIMESTAMP
			c.Value = buildValue(MYSQL_TYPE_TIMESTAMP, values[i])
		} else {
			c.Value = values[i]
		}
		columns = append(columns, c)
	}
	return &Row{
		After:  nil,
		Before: columns,
	}
}

//创建只包含After的Row
func newAfterRow(columnName []string, pkNames []string, values []string) *Row {
	if len(columnName) != len(values) {
		log.Fatalf("newAfterRow:len(columnName)=%d,len(values)=%d,",
			len(columnName), len(values))
	}

	columns := make([]*Column, 0, len(columnName))
	for i, name := range columnName {
		c := &Column{
			Name: name,
			//Value: values[i], //columnName和values是一一对应的
		}
		for j := 0; j < len(pkNames); j++ {
			if pkNames[j] == name {
				c.IsPk = true
				break
			}
		}
		if EscapeName(name) == DrcTimeColumnName {
			c.BinlogType = MYSQL_TYPE_TIMESTAMP
			c.Value = buildValue(MYSQL_TYPE_TIMESTAMP, values[i])
		} else {
			c.Value = values[i]
		}
		columns = append(columns, c)
	}
	return &Row{
		After:  columns,
		Before: nil,
	}
}

//创建包含Before和After的Row
func newBeforeAndAfterRow(columnName []string, pkNames []string, beforeValues, afterValues []string) *Row {
	if len(columnName) != len(beforeValues) || len(columnName) != len(afterValues) {
		log.Fatalf("newBeforeAndAfterRow:len(columnName)=%d,len(beforeValues)=%d,len(afterValues)=%d",
			len(columnName), len(beforeValues), len(afterValues))
	}

	beforeColumns := make([]*Column, 0, 16)
	afterColumns := make([]*Column, 0, 16)
	for i, name := range columnName {
		bc := &Column{
			Name: name,
		}
		ac := &Column{
			Name: name,
		}
		for j := 0; j < len(pkNames); j++ {
			if pkNames[j] == name {
				bc.IsPk = true
				ac.IsPk = true
				break
			}
		}

		if EscapeName(name) == DrcTimeColumnName {
			bc.BinlogType = MYSQL_TYPE_TIMESTAMP
			bc.Value = buildValue(MYSQL_TYPE_TIMESTAMP, beforeValues[i])
			ac.BinlogType = MYSQL_TYPE_TIMESTAMP
			ac.Value = buildValue(MYSQL_TYPE_TIMESTAMP, afterValues[i])
		} else {
			bc.Value = beforeValues[i]
			ac.Value = afterValues[i]
		}
		beforeColumns = append(beforeColumns, bc)
		afterColumns = append(afterColumns, ac)
	}
	return &Row{
		After:  afterColumns,
		Before: beforeColumns,
	}
}

func TestRowEqual(t *testing.T) {
	a := []interface{}{495958, 0, 495371, "72226157825-18506140955", "04714757182"}
	b := []interface{}{495958, 0, 495371, "72226157825-18506140955", "04714757182"}

	var equal = true
	for i, v := range a {
		if b[i] != v {
			equal = false
			break
		}
	}
	assert.Equal(t, equal, true)
}

func TestEscapeName(t *testing.T) {
	names := []string{"my_table", `"my_table"`, "`my_table`"}
	for _, name := range names {
		escaped := EscapeName(name)
		assert.Equal(t, escaped, "`my_table`")
	}
}
