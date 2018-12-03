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

package utils

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

const (
	OffsetNewest     int64 = -1
	OffsetOldest     int64 = -2
	OffsetCheckpoint int64 = -3
)

func GetMysqlVar(mysqlHost string, mysqlPort int, mysqlUser string, mysqlPasswd string, varName string) (string, error) {
	// 连接MySQL
	connectStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/", mysqlUser, mysqlPasswd, mysqlHost, mysqlPort)

	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		return "", err
	}
	defer db.Close()

	var result sql.NullString
	sql := fmt.Sprintf("SELECT @@%s", varName)

	err = db.QueryRow(sql).Scan(&result)
	if err != nil {
		return "", err
	}

	if !result.Valid {
		return "NULL", nil
	}

	return result.String, nil
}

func CheckTableExist(mysqlHost string, mysqlPort int, mysqlUser string,
	mysqlPasswd string, tableSchema string, tableName string) (bool, error) {
	// 连接MySQL
	connectStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/", mysqlUser, mysqlPasswd, mysqlHost, mysqlPort)

	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		return false, err
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT count(*) FROM `INFORMATION_SCHEMA`.`TABLES` "+
		"WHERE `TABLE_SCHEMA`='%s' AND `TABLE_NAME`='%s';", tableSchema, tableName)

	var count int
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		return false, err
	}
	switch count {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		str := fmt.Sprintf("CheckTableExist:count is %d,query is %s", count, query)
		panic(str)
	}
}

func GetConsumeOffset(mysqlHost string, mysqlPort int, mysqlUser string,
	mysqlPasswd string, tableSchema string, tableName string, kafkaTopic string) (int64, error) {
	// 连接MySQL
	connectStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/", mysqlUser, mysqlPasswd, mysqlHost, mysqlPort)

	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT MAX(lwm_offset) from `%s`.`%s` WHERE "+
		"`lwm_offset` != -3 and `kafka_topic` = '%s';", tableSchema, tableName, kafkaTopic)

	var result sql.NullInt64
	err = db.QueryRow(query).Scan(&result)
	if err != nil {
		return 0, err
	}

	//返回结果为NULL时，从最旧位置开始消费
	if !result.Valid {
		return OffsetOldest, nil
	}
	//返回有效位置时，从有效位置的下一个位置开始消费
	if 0 <= result.Int64 {
		return result.Int64 + 1, nil
	}
	//从最新位置或最旧位置开始消费
	if result.Int64 == OffsetOldest || result.Int64 == OffsetNewest {
		return result.Int64, nil
	} else {
		panic(fmt.Sprintf("offset is illegal,offset:%d", result.Int64))
	}
}
