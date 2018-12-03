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
	"flag"
	"fmt"
	"log"
	"testing"

	"context"

	"strings"

	"strconv"
	"time"

	"github.com/stretchr/testify/suite"
)

var mysqlAddr = flag.String("mysql-addr", "127.0.0.1:3306", "MySQL address")

type ExecTestSuite struct {
	suite.Suite
	db           *sql.DB
	databaseName string
	tableName    string
}

func (s *ExecTestSuite) SetupTest() {
	var err error
	//initdb
	err = SetCheckpointDBandTableName("10000")
	s.Require().Nil(err)
	s.db, err = sql.Open("mysql", fmt.Sprintf("root:123456@tcp(%s)/?parseTime=true&loc=Local&interpolateParams=true", *mysqlAddr))
	if err != nil {
		panic(err)
	}
	_, err = s.db.Exec(fmt.Sprintf("drop database if exists %s", TestDatabase))
	s.Require().Equal(nil, err)
	s.Equal(nil, err)
	initCheckpointTable(s.db)
	createTestTable(s.db)
	s.databaseName = TestDatabase
	s.tableName = TestTable
}

func initCheckpointTable(db *sql.DB) {
	var err error
	//init checkpoint table
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", CheckpointDatabase)
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s(
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
	initTableSQL := fmt.Sprintf("INSERT INTO %s.%s(id,binlog_file_name,position) VALUES(?,?,?)", CheckpointDatabase, CheckpointTable)
	//创建数据库
	_, err = db.Exec(createDBSQL)
	if err != nil {
		panic(err)
	}
	//创建表
	_, err = db.Exec(createTableSQL)
	if err != nil {
		panic(err)
	}
	//删除全部记录
	_, err = db.Exec(deleteTableSQL)
	if err != nil {
		panic(err)
	}
	startTime := time.Now()
	//插入1000条记录
	for i := 0; i < CheckpointTableCount; i++ {
		_, err = db.Exec(initTableSQL, i, "init_name", 0)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("insert time is %v\n", time.Now().Sub(startTime))
}

func (s *ExecTestSuite) TestInsertNoConflict() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set 
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:               s.db,
		strategyType:     OverwriteStrategy,
		updateTimeColumn: "`drc_update_time`",
		resultC:          make(chan ExecResult),
	}
	trx := newInsetNoConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Equal(nil, result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abc", "-128", "mytest", "2018-05-01T00:00:00+08:00", "-32768", "-8388608",
				"-2147483648", "-9223372036854775808", "2018-05-01T11:12:00+08:00", "2018-06-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
			{"abc", "-128", "mytest", "2015-05-01T00:00:00+08:00", "-32768", "-8388608",
				"100", "-9223372036854775808", "2015-05-01T11:12:00+08:00", "2018-05-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
		})
}

func (s *ExecTestSuite) TestInsertConflictWithTimestamp() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set 
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:               s.db,
		strategyType:     TimeOverwriteStrategy,
		updateTimeColumn: "`drc_update_time`",
		resultC:          make(chan ExecResult),
	}
	trx := newInsetWithConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Equal(nil, result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abc", "-128", "mytest", "2018-05-01T00:00:00+08:00", "-32768", "-8388608",
				"-2147483648", "-9223372036854775808", "2018-05-01T11:12:00+08:00", "2018-06-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
		})
}

func (s *ExecTestSuite) TestInsertConflictWithIgnore() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:               s.db,
		strategyType:     OverwriteStrategy,
		updateTimeColumn: "`drc_update_time`",
		resultC:          make(chan ExecResult),
	}
	trx := newInsetNoConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abc", "-128", "mytest", "2018-05-01T00:00:00+08:00", "-32768", "-8388608",
				"-2147483648", "-9223372036854775808", "2018-05-01T11:12:00+08:00", "2018-06-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
			{"abc", "-128", "mytest", "2015-05-01T00:00:00+08:00", "-32768", "-8388608",
				"100", "-9223372036854775808", "2015-05-01T11:12:00+08:00", "2018-05-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
		})
}

func (s *ExecTestSuite) TestInsertConflictWithOverWrite() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:               s.db,
		strategyType:     OverwriteStrategy,
		updateTimeColumn: "`drc_update_time`",
		resultC:          make(chan ExecResult),
	}
	trx := newInsetWithConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abcd", "127", "mytest2", "2018-05-01T00:00:00+08:00", "32767",
				"8388607", "-2147483648", "9223372036854775807", "2015-05-01T11:12:00+08:00",
				"2018-05-01T11:13:00+08:00", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
				"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
				"myvarbinary"},
		})
}

func (s *ExecTestSuite) TestUpdateNoConflict() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:               s.db,
		strategyType:     OverwriteStrategy,
		updateTimeColumn: "`drc_update_time`",
		resultC:          make(chan ExecResult),
	}
	trx := newUpdateNoConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)

	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abcd", "127", "mytest", "2015-05-01T00:00:00+08:00", "-32768",
				"8388607", "-2147483648", "9223372036854775807", "2018-06-01T11:12:00+08:00",
				"2018-07-01T11:13:00+08:00", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
				"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
				"myvarbinary"},
		})
}

func (s *ExecTestSuite) TestUpdateConflictWithTimestamp() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:               s.db,
		strategyType:     TimeOverwriteStrategy,
		updateTimeColumn: "`drc_update_time`",
		resultC:          make(chan ExecResult),
	}
	trx := newUpdateWithConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abcd", "127", "mytest", "2015-05-01T00:00:00+08:00", "-32768",
				"8388607", "-2147483648", "9223372036854775807", "2018-06-01T11:12:00+08:00",
				"2018-07-01T11:13:00+08:00", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
				"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
		})
}

func (s *ExecTestSuite) TestUpdateConflictWithIgnore() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:           s.db,
		strategyType: IgnoreStrategy,
		resultC:      make(chan ExecResult),
	}
	trx := newUpdateWithConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abc", "-128", "mytest", "2018-05-01T00:00:00+08:00", "-32768", "-8388608",
				"-2147483648", "-9223372036854775808", "2018-05-01T11:12:00+08:00", "2018-06-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
		})
}

func (s *ExecTestSuite) TestUpdateConflictWithOverwrite() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:           s.db,
		strategyType: OverwriteStrategy,
		resultC:      make(chan ExecResult),
	}
	trx := newUpdateWithConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abcd", "127", "mytest", "2015-05-01T00:00:00+08:00", "-32768",
				"8388607", "-2147483648", "9223372036854775807", "2018-06-01T11:12:00+08:00",
				"2018-07-01T11:13:00+08:00", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
				"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
				"myvarbinary"},
		})
}

func (s *ExecTestSuite) TestUpdateNoPk() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	if err != nil {
		s.Require().Equal(nil, err)
	}
	opts := &ExecOpt{
		db:           s.db,
		strategyType: OverwriteStrategy,
		resultC:      make(chan ExecResult),
	}
	trx := newUpdateNoPk()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)

	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abc", "-128", "mytest", "2018-05-01T00:00:00+08:00", "-32768", "-8388608",
				"-2147483648", "-9223372036854775808", "2018-05-01T11:12:00+08:00", "2018-06-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
			{"abcd", "127", "mytest", "2015-05-01T00:00:00+08:00", "-32768",
				"8388607", "2147483647", "9223372036854775807", "2018-06-01T11:12:00+08:00",
				"2018-07-01T11:13:00+08:00", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
				"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
				"myvarbinary"},
		})
}

func (s *ExecTestSuite) TestDeleteNoConflict() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	s.Require().Nil(err)

	opts := &ExecOpt{
		db:           s.db,
		strategyType: OverwriteStrategy,
		resultC:      make(chan ExecResult),
	}
	trx := newDeleteNoConflict()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
}

func (s *ExecTestSuite) TestDeleteNoPk() {
	s.ClearTable()
	insertSQL := fmt.Sprintf(`
insert into %s.%s set
varchar_var="abc",
tinyint_var="-128",
text_var="mytest",
date_var="2018-05-01",
smallint_var="-32768",
mediumint_var="-8388608",
int_var="-2147483648",
bigint_var="-9223372036854775808",
datetime_var="2018-05-01 11:12:00",
drc_update_time="2018-06-01 11:13:00",
time_var="11:12:00",
year_var="1987",
char_var="mychar",
tinyblob_var="mytinyblob",
tinytext_var="mytinytext",
blob_var="myblob",
mediumblob_var="mymediumblob",
mediumtext_var="mymediumtext",
longblob_var="longblob",
longtext_var="longtext",
enum_var="1",
set_var="1,2",
bool_var="1",
varbinary_var="myvarbinary"
`, s.databaseName, s.tableName)

	_, err := s.db.Exec(insertSQL)
	s.Require().Nil(err)

	opts := &ExecOpt{
		db:           s.db,
		strategyType: OverwriteStrategy,
		resultC:      make(chan ExecResult),
	}
	trx := newDeleteNoPk()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
	s.CheckTableData([]string{
		"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var", "mediumint_var",
		"int_var", "bigint_var", "datetime_var", "drc_update_time", "time_var", "year_var",
		"char_var", "tinyblob_var", "tinytext_var", "blob_var", "mediumblob_var", "mediumtext_var",
		"longblob_var", "longtext_var", "enum_var", "set_var", "bool_var", "varbinary_var"},
		[][]interface{}{
			{"abc", "-128", "mytest", "2018-05-01T00:00:00+08:00", "-32768", "-8388608",
				"-2147483648", "-9223372036854775808", "2018-05-01T11:12:00+08:00", "2018-06-01T11:13:00+08:00", "11:12:00", "1987",
				"mychar", "mytinyblob", "mytinytext", "myblob", "mymediumblob", "mymediumtext",
				"longblob", "longtext", "1", "1,2", "1", "myvarbinary"},
		})
}

//insert+update+delete, 没有冲突
func (s *ExecTestSuite) TestExecBinlogTrx3() {
	opts := &ExecOpt{
		db:           s.db,
		strategyType: OverwriteStrategy,
		resultC:      make(chan ExecResult),
	}
	trx := newDMLBinlogTrx()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
}

//ddl
func (s *ExecTestSuite) TestExecBinlogTrx4() {
	opts := &ExecOpt{
		db:           s.db,
		strategyType: OverwriteStrategy,
		resultC:      make(chan ExecResult),
		allowDDL:     true,
	}
	trx := newDDLBinlogTrx()
	go ExecBinlogTrx(context.Background(), opts, trx)
	result := <-opts.resultC
	s.Require().Nil(result.Err)
}

func createTestTable(db *sql.DB) {
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", TestDatabase)
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s (
	varchar_var VARCHAR(20) NOT NULL ,
	tinyint_var TINYINT NOT NULL ,
	text_var TEXT NOT NULL ,
	date_var DATE NOT NULL ,
	smallint_var SMALLINT NOT NULL ,
	mediumint_var MEDIUMINT NOT NULL ,
	int_var INT NOT NULL ,
	bigint_var BIGINT NOT NULL ,
	datetime_var DATETIME NOT NULL ,
	drc_update_time TIMESTAMP NOT NULL ,
	time_var TIME NOT NULL ,
	year_var YEAR NOT NULL ,
	char_var CHAR(10) NOT NULL ,
	tinyblob_var TINYBLOB NOT NULL ,
	tinytext_var TINYTEXT NOT NULL ,
	blob_var BLOB NOT NULL ,
	mediumblob_var MEDIUMBLOB NOT NULL ,
	mediumtext_var MEDIUMTEXT NOT NULL ,
	longblob_var LONGBLOB NOT NULL ,
	longtext_var LONGTEXT NOT NULL ,
	enum_var ENUM('1', '2', '3') NOT NULL ,
	set_var SET('1', '2', '3') NOT NULL ,
	bool_var BOOL NOT NULL ,
	varbinary_var VARBINARY(20) NOT NULL,
	PRIMARY KEY(int_var)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;`, TestDatabase, TestTable)

	//创建数据库
	_, err := db.Exec(createDBSQL)
	if err != nil {
		panic(err)
	}
	//创建表
	_, err = db.Exec(createTableSQL)
	if err != nil {
		panic(err)
	}
}

//todo check pk
//insert+update+delete
func newDMLBinlogTrx() *Trx {
	insertEvent := newInsertEvent( //列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//值
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "-2147483648", "-9223372036854775808", "2015-05-01 11:12:00",
			"1525144380000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	updateEvent := newUpdateEvent(
		//列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//前镜像
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "-2147483648", "-9223372036854775808", "2015-05-01 11:12:00",
			"1525144380000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"},
		//后镜像
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "2147483647", "9223372036854775807", "2018-06-01 11:12:00",
			"1530414780000", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	deleteEvent := newDeleteEvent(
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//值
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "2147483647", "9223372036854775807", "2018-06-01 11:12:00",
			"1530414780000", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})

	return &Trx{
		Events:         []*Event{insertEvent, updateEvent, deleteEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newInsetNoConflict() *Trx {
	insertEvent := newInsertEvent( //列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//值
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "100", "-9223372036854775808", "2015-05-01 11:12:00",
			"1525144380000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{insertEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newInsetWithConflict() *Trx {
	insertEvent := newInsertEvent( //列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//abcd 127
		[]string{"abcd", "127", "mytest2", "2018-05-01", "32767",
			"8388607", "-2147483648", "9223372036854775807", "2015-05-01 11:12:00",
			"1525144380000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{insertEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newUpdateNoConflict() *Trx {
	updateEvent := newUpdateEvent(
		//列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//前镜像
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "-2147483648", "-9223372036854775808", "2018-05-01 11:12:00",
			"1527822780000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"},
		//后镜像
		[]string{"abcd", "127", "mytest", "2015-05-01", "-32768",
			"8388607", "-2147483648", "9223372036854775807", "2018-06-01 11:12:00",
			"1530414780000", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{updateEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newUpdateWithConflict() *Trx {
	updateEvent := newUpdateEvent(
		//列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//前镜像
		[]string{"abcde", "127", "mytest", "2015-05-01", "-32768",
			"-8388608", "-2147483648", "-9223372036854775808", "2018-05-01 11:12:00",
			"1527822780000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"},
		//后镜像
		[]string{"abcd", "127", "mytest", "2015-05-01", "-32768",
			"8388607", "-2147483648", "9223372036854775807", "2018-06-01 11:12:00",
			"1530414780000", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{updateEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newUpdateNoPk() *Trx {
	updateEvent := newUpdateEvent(
		//列名称
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//前镜像
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "2147483647", "-9223372036854775808", "2018-05-01 11:12:00",
			"1527822780000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"},
		//后镜像
		[]string{"abcd", "127", "mytest", "2015-05-01", "-32768",
			"8388607", "2147483647", "9223372036854775807", "2018-06-01 11:12:00",
			"1530414780000", "11:12:00", "1988", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{updateEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newDeleteNoConflict() *Trx {
	deleteEvent := newDeleteEvent(
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//值
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "-2147483648", "-9223372036854775808", "2018-05-01 11:12:00",
			"1527822780000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{deleteEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newDeleteNoPk() *Trx {
	deleteEvent := newDeleteEvent(
		[]string{
			"varchar_var", "tinyint_var", "text_var", "date_var", "smallint_var",
			"mediumint_var", "int_var", "bigint_var", "datetime_var",
			"drc_update_time", "time_var", "year_var", "char_var", "tinyblob_var", "tinytext_var", "blob_var",
			"mediumblob_var", "mediumtext_var", "longblob_var", "longtext_var", "enum_var", "set_var", "bool_var",
			"varbinary_var"},
		[]string{"int_var"},
		//值
		[]string{"abc", "-128", "mytest", "2015-05-01", "-32768",
			"-8388608", "2147483647", "-9223372036854775808", "2018-05-01 11:12:00",
			"1527822780000", "11:12:00", "1987", "mychar", "mytinyblob", "mytinytext", "myblob",
			"mymediumblob", "mymediumtext", "longblob", "longtext", "1", "1,2", "1",
			"myvarbinary"})
	return &Trx{
		Events:         []*Event{deleteEvent},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func newDDLBinlogTrx() *Trx {
	createSQL := `
		/*mysql-applier*/CREATE TABLE IF NOT EXISTS ddl_test_table(
			server_id char(36) NOT NULL,
			binlog_file_name varchar(256) NOT NULL,
			position bigint(20) NOT NULL,
			update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (server_id)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
		`
	event := newQueryEvent(createSQL)
	return &Trx{
		Events:         []*Event{event},
		BinlogFileName: "mysql_bin_00001.log",
		Position:       1,
	}
}

func (s *ExecTestSuite) ClearTable() {
	deleteTableSQL := fmt.Sprintf("DELETE FROM %s.%s;", s.databaseName, s.tableName)
	//删除全部记录
	_, err := s.db.Exec(deleteTableSQL)
	if err != nil {
		panic(err)
	}
}

func (s *ExecTestSuite) CheckTableData(columnNames []string, expectValue [][]interface{}) {
	//获取数据
	selectColumns := strings.Join(columnNames, ", ")
	selectSQL := fmt.Sprintf(`
	select %s from %s.%s
`, selectColumns, s.databaseName, s.tableName)

	rows, err := s.db.Query(selectSQL)
	s.Require().Nil(err)
	defer rows.Close()

	i := 0
	for rows.Next() {
		currentRow := make([]interface{}, len(columnNames))
		rowValues := make([]sql.NullString, len(columnNames))
		for i := range rowValues {
			currentRow[i] = &rowValues[i]
		}

		err := rows.Scan(currentRow...)
		s.Require().Nil(err)

		//对比数据
		for j, v := range expectValue[i] {
			if rowValues[j].Valid && v.(string) != rowValues[j].String {
				s.T().Fatalf("expect:%v,but is %v", v, rowValues[j].String)
			} else if !rowValues[j].Valid {
				s.Require().Equal("NULL", rowValues[j].String)
			}
		}
		i++
	}
}

//TearDownSuite
func (s *ExecTestSuite) TearDownSuite() {
	_, err := s.db.Exec(fmt.Sprintf("drop database if exists %s", s.databaseName))
	s.Require().Nil(err)
	_, err = s.db.Exec(fmt.Sprintf("drop database if exists %s", CheckpointDatabase))
	s.Require().Nil(err)
	err = s.db.Close()
	s.Require().Nil(err)
}

func TestExecTestSuite(t *testing.T) {
	suiteTester := new(ExecTestSuite)
	suite.Run(t, suiteTester)
}

func buildValue(columnType ColumnType, columnValue string) interface{} {
	var arg interface{}
	switch columnType {
	case MYSQL_TYPE_TIMESTAMP:
		v, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		// rdp记录timestamp的单位是ms
		arg = time.Unix(v/1000, 0)
	default:
		arg = columnValue
	}
	return arg
}
