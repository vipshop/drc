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

//https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType

type ColumnType byte

const (
	MYSQL_TYPE_DECIMAL ColumnType = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_TIMESTAMP2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2

	MYSQL_TYPE_JSON        = 245
	MYSQL_TYPE_NEWDECIMAL  = 246
	MYSQL_TYPE_ENUM        = 247
	MYSQL_TYPE_SET         = 248
	MYSQL_TYPE_TINY_BLOB   = 249
	MYSQL_TYPE_MEDIUM_BLOB = 250
	MYSQL_TYPE_LONG_BLOB   = 251
	MYSQL_TYPE_BLOB        = 252
	MYSQL_TYPE_VAR_STRING  = 253
	MYSQL_TYPE_STRING      = 254
	MYSQL_TYPE_GEOMETRY    = 255
)
