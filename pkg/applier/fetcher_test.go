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
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vipshop/drc/pkg/applier/rdp_messages"
)

func simulateTrx() *Trx {
	var c1, c2, c3, c4, c5 Column
	var c11, c12, c13, c14, c15 Column
	var before, after []*Column
	var r Row
	var e Event
	var trx Trx

	c1.Name = "id"
	c1.Type = "int(10) unsigned"
	c1.Value = uint32(1)
	c1.IsPk = true
	c1.BinlogType = MYSQL_TYPE_LONG

	c2.Name = "r"
	c2.Type = "int(10) unsigned"
	c2.Value = uint32(0)
	c2.BinlogType = MYSQL_TYPE_LONG

	c3.Name = "k"
	c3.Type = "int(10) unsigned"
	c3.Value = int32(-49696)
	c3.BinlogType = MYSQL_TYPE_LONG

	c4.Name = "c"
	c4.Type = "varchar(120)"
	c4.Value = "93463014298-37432558903"
	c4.BinlogType = MYSQL_TYPE_VARCHAR

	c5.Name = "pad"
	c5.Type = "varchar(60)"
	c5.Value = "52697353395"
	c5.BinlogType = MYSQL_TYPE_VARCHAR

	before = append(before, &c1, &c2, &c3, &c4, &c5)

	c11, c12, c13, c14, c15 = c1, c2, c3, c4, c5
	c11.Value = uint32(2)
	c13.Value = uint32(50019)
	c14.Value = "68337857930-12328386514"
	c15.Value = "12345678"
	after = append(after, &c11, &c12, &c13, &c14, &c15)

	r.Before = before
	r.After = after

	position := uint64(400)
	now := uint64(time.Now().Unix())

	e.DatabaseName = "test"
	e.TableName = "sbtest1"
	e.EventType = UPDATE_ROWS_EVENT
	e.Timestamp = uint64(now)
	e.TimestampOfReceipt = uint64(now) + 1
	e.Position = position
	e.NextPosition = position + 10
	e.BinlogFileName = "binlog.001122"
	e.ServerId = 123456
	e.SqlStatement = ""
	e.Rows = append(e.Rows, &r, &r)

	trx.Gtid = "47fbb51c-266d-11e8-a07f-244427b6b60e:1"
	trx.Seq = 1
	trx.Position = position
	trx.BinlogFileName = "binlog.001122"
	trx.NextPosition = position + 10
	trx.NextBinlogFileName = "binlog.001122"
	trx.LastCommitted = 0
	trx.Events = append(trx.Events, &e, &e, &e, &e, &e)

	return &trx
}

// 将1个Trx分为多个Trx
func splitTrx(originalTrx *Trx, n int) []*Trx {
	var trxs []*Trx
	l := len(originalTrx.Events)
	delta := l / n
	if delta == 0 {
		// 每个trx最少一个trx
		delta = 1
	}
	i := 0
	for i < l {
		// 先复制originalTrx的所有字段， 再修改Events字段
		trx := *originalTrx
		var events []*Event
		if i+delta >= l {
			events = originalTrx.Events[i:l]
		} else {
			events = originalTrx.Events[i : i+delta]
		}
		trx.Events = events
		trxs = append(trxs, &trx)
		i += delta
	}
	return trxs
}

func constructValue(value interface{}) string {
	switch v := value.(type) {
	case int32:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case string:
		return v
	}
	return ""
}

func constructColumn(c *Column, column *rdp_messages.Column) {
	column.Name = []byte(c.Name)
	column.Type = []byte(c.Type)
	column.Value = []byte(constructValue(c.Value))

	column.BinlogType = proto.Uint32(uint32(c.BinlogType))
	if c.IsPk {
		column.Key = proto.String("PRI")
	}
}

func constructRow(r *Row, row *rdp_messages.Row) {
	for _, c := range r.Before {
		var column rdp_messages.Column
		constructColumn(c, &column)
		row.Before = append(row.Before, &column)
	}
	for _, c := range r.After {
		var column rdp_messages.Column
		constructColumn(c, &column)
		row.After = append(row.After, &column)
	}
}

func constructEvent(e *Event, event *rdp_messages.Event) {
	event.DatabaseName = []byte(e.DatabaseName)
	event.TableName = []byte(e.TableName)
	event.EventType = proto.Uint32(uint32(e.EventType))
	// event.SchemaId = 0

	event.Timestamp = proto.Uint64(e.Timestamp)
	event.TimestampOfReceipt = proto.Uint64(e.TimestampOfReceipt)
	event.Position = proto.Uint64(e.Position)
	event.NextPosition = proto.Uint64(e.NextPosition)
	event.BinlogFileName = []byte(e.BinlogFileName)
	event.ServerId = proto.Uint64(e.ServerId)
	event.SqlStatement = []byte(e.SqlStatement)
	if e.SqlStatement != "" {
		event.Ddl = rdp_messages.DDL_kDDLChanged.Enum()
	} else {
		event.Ddl = rdp_messages.DDL_kDDLNoChage.Enum()
	}

	for _, r := range e.Rows {
		var row rdp_messages.Row
		constructRow(r, &row)
		event.Rows = append(event.Rows, &row)
	}

}

func constructTransaction(trx *Trx, transaction *rdp_messages.Transaction) {
	// trx
	transaction.Gtid = []byte(trx.Gtid)
	transaction.Seq = proto.Uint32(uint32(trx.Seq))
	transaction.Position = proto.Uint64(trx.Position)
	transaction.NextPosition = proto.Uint64(trx.NextPosition)
	transaction.BinlogFileName = []byte(trx.BinlogFileName)
	transaction.NextBinlogFileName = []byte(trx.NextBinlogFileName)

	for _, e := range trx.Events {
		var event rdp_messages.Event
		constructEvent(e, &event)
		transaction.Events = append(transaction.Events, &event)
	}

}

func generateKafkaMessage(trx *Trx, needCompress bool) ([]byte, error) {
	return doGenerateKafkaMessage(trx, needCompress, 0, 0)
}

func generateMultiKafkaMessage(orginalTrx *Trx, needCompress bool, n int) ([][]byte, error) {
	var bufs [][]byte
	trxs := splitTrx(orginalTrx, n)
	for i, trx := range trxs {
		splitFlag := 1
		if i == len(trxs)-1 {
			// 最后一个分包
			splitFlag = 2
		}
		buf, err := doGenerateKafkaMessage(trx, needCompress, int32(splitFlag), uint64(i))
		if err != nil {
			return nil, err
		}
		bufs = append(bufs, buf)
	}
	return bufs, nil
}

func doGenerateKafkaMessage(trx *Trx, needCompress bool, splitFlag int32, splitSeq uint64) ([]byte, error) {
	var err error

	var transaction rdp_messages.Transaction
	var pkg rdp_messages.KafkaPkg
	var vmsPkg rdp_messages.VMSMessage

	constructTransaction(trx, &transaction)

	// kafkaPkg
	var buf []byte
	buf, err = proto.Marshal(&transaction)
	if err != nil {
		log.Printf("generateKafkaMessage: marshal transaction error: %s", err)
		return nil, err
	}

	if needCompress {
		srcLen := len(buf)
		dst := make([]byte, 1024*1024*10)
		n, err := lz4.CompressBlock(buf, dst, 0)
		if err != nil {
			log.Printf("generateKafkaMessage: compress error: %s", err)
			return nil, err
		}
		buf = dst[:n]
		pkg.Flag = proto.Uint32(uint32(rdp_messages.KafkaPkgFlag_kKfkPkgCompressData))
		pkg.SourceDataLen = proto.Uint64(uint64(srcLen))
	}

	pkg.Epoch = proto.Uint64(1)
	pkg.TransSeqNo = proto.Uint64(trx.Seq)
	pkg.Gtid = []byte(trx.Gtid)
	pkg.SeqNo = proto.Uint64(splitSeq)
	pkg.SplitFlag = proto.Int32(splitFlag)

	pkg.Data = buf

	// vmsPkg
	buf, err = proto.Marshal(&pkg)
	if err != nil {
		log.Printf("generateKafkaMessage: marshal kafka error: %s", err)
		return nil, err
	}
	vmsPkg.Payload = buf

	buf, err = proto.Marshal(&vmsPkg)
	if err != nil {
		log.Printf("generateKafkaMessage: marshal vms error: %s", err)
		return nil, err
	}

	return buf, nil
}

func TestUnmarshalCompressedKafkaMessage(t *testing.T) {
	var err error
	trx := simulateTrx()

	decoder := NewRdpDecoder(500)
	message, err := generateKafkaMessage(trx, true)
	require.Nil(t, err)

	err = decoder.feed(message, "", 0)
	require.Nil(t, err)

	actualTrx, err := decoder.peek()
	require.Nil(t, err)

	actualTrx.fetchedTime = time.Time{}
	require.Equal(t, *trx, *actualTrx)

}

func TestUnmarshalUncompressedKafkaMessage(t *testing.T) {
	var err error
	trx := simulateTrx()

	decoder := NewRdpDecoder(500)
	message, err := generateKafkaMessage(trx, false)
	require.Nil(t, err)

	err = decoder.feed(message, "", 0)
	require.Nil(t, err)

	actualTrx, err := decoder.peek()
	require.Nil(t, err)

	actualTrx.fetchedTime = time.Time{}
	require.Equal(t, *trx, *actualTrx)

}

func TestSplitedKafaMessage(t *testing.T) {
	var err error
	trx := simulateTrx()

	decoder := NewRdpDecoder(500)
	messages, err := generateMultiKafkaMessage(trx, false, 5)
	require.Nil(t, err)

	for i, message := range messages {
		err = decoder.feed(message, "", int64(i))
		require.Nil(t, err)
		if i != len(messages)-1 {
			// 不是最后一个分包的话，应该不能peek
			require.False(t, decoder.canPeek())
		}
	}
	require.True(t, decoder.canPeek())

	actualTrx, err := decoder.peek()
	require.Nil(t, err)

	actualTrx.fetchedTime = time.Time{}
	trx.EndOffset = int64(len(messages) - 1)
	require.Equal(t, *trx, *actualTrx)

}

func TestNeedFilter(t *testing.T) {
	err := SetCheckpointDBandTableName("1000")
	require.Nil(t, err)

	fetcher := NewFetcher(&ApplierConfig{AllowDDL: true})

	trx0 := Trx{
		Gtid: "",
	}
	require.True(t, fetcher.needFilter(&trx0))

	event := &Event{
		SqlStatement: "drop tableName a",
	}
	trx1 := Trx{
		Gtid:   "1",
		Events: []*Event{event},
	}
	require.False(t, fetcher.needFilter(&trx1))

	event = &Event{
		SqlStatement: "/*mysql-applier*/drop tableName a",
	}
	trx2 := Trx{
		Gtid:   "2",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx2))

	event = &Event{
		DatabaseName: CheckpointDatabase,
		TableName:    CheckpointTable,
	}
	trx3 := Trx{
		Gtid:   "3",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx3))

	// 测试GRANT语句
	event = &Event{
		SqlStatement: "GRANT SELECT, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, SHOW VIEW, EXECUTE,  EVENT ON *.* TO 'rdp'@'10.%';",
	}
	trx4 := Trx{
		Gtid:   "4",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx4))

	// 测试FLUSH语句
	event = &Event{
		SqlStatement: "Flush PRIVILEGES;",
	}
	trx5 := Trx{
		Gtid:   "5",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx5))

	// 测试CREATE USER语句
	event = &Event{
		SqlStatement: "create user 'root'@'%';",
	}
	trx6 := Trx{
		Gtid:   "6",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx6))

	// 测试CREATE DEFINER ... TRIGGER
	event = &Event{
		SqlStatement: "CREATE DEFINER=`dba`@`localhost` TRIGGER vip_sales_infoq.sales_front_index_AI_oak AFTER ...",
	}
	trx7 := Trx{
		Gtid:   "7",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx7))

	// 测试CREATE TRIGGER
	event = &Event{
		SqlStatement: "CREATE TRIGGER vip_sales_infoq.sales_front_index_AI_oak AFTER ...",
	}
	trx8 := Trx{
		Gtid:   "8",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx8))

	// 测试CREATE PROCEDURE
	event = &Event{
		SqlStatement: "CREATE PROCEDURE vip_sales_infoq.sales_front_index_AI_oak ...",
	}
	trx9 := Trx{
		Gtid:   "9",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx9))

	// 测试DROP TRIGGER
	event = &Event{
		SqlStatement: "DROP TRIGGER vip_sales_infoq.sales_front_index_AI_oak;",
	}
	trx10 := Trx{
		Gtid:   "10",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx10))

	// 测试DROP PROCEDURE
	event = &Event{
		SqlStatement: "DROP PROCEDURE vip_sales_infoq.sales_front_index_AI_oak;",
	}
	trx12 := Trx{
		Gtid:   "12",
		Events: []*Event{event},
	}
	require.True(t, fetcher.needFilter(&trx12))

}

// 构造随机内容的已压缩的byte数组
func generateRandSrc() ([]byte, []byte, error) {
	size := rand.Intn(1024*1024) + 1024
	origin := make([]byte, size)
	for i := 0; i < len(origin); i++ {
		origin[i] = byte(rand.Intn(5))
	}

	//进行压缩
	dst := make([]byte, 1024*1024*10)
	n, err := lz4.CompressBlock(origin, dst, 0)
	if err != nil {
		return nil, nil, err
	}

	return origin, dst[:n], err
}

// 构造重复内容的已压缩的byte数组
func generateRepeatedSrc(delta int) ([]byte, []byte, error) {
	size := rand.Intn(1024*1024) + 1024
	origin := make([]byte, size)
	v := 0
	for i := 0; i < len(origin); i += delta {
		for j := 0; j < delta && i+j < len(origin); j++ {
			origin[i+j] = byte(v)
		}
		v++
	}
	//log.Print(origin)
	// 进行压缩
	dst := make([]byte, 1024*1024*10)
	n, err := lz4.CompressBlock(origin, dst, 0)
	if err != nil {
		return nil, nil, err
	}

	return origin, dst[:n], err
}

func TestBuildValue(t *testing.T) {
	decoder := NewRdpDecoder(500)

	tinyValue := decoder.buildValue(MYSQL_TYPE_TINY, "255")
	assert.Equal(t, tinyValue.(uint8), uint8(255))
	tinyValue = decoder.buildValue(MYSQL_TYPE_TINY, "-128")
	assert.Equal(t, tinyValue.(int8), int8(-128))

	shortValue := decoder.buildValue(MYSQL_TYPE_SHORT, "32767")
	assert.Equal(t, shortValue.(uint16), uint16(32767))
	shortValue = decoder.buildValue(MYSQL_TYPE_SHORT, "-32768")
	assert.Equal(t, shortValue.(int16), int16(-32768))

	mediumValue := decoder.buildValue(MYSQL_TYPE_INT24, "8388607")
	assert.Equal(t, mediumValue.(uint32), uint32(8388607))
	mediumValue = decoder.buildValue(MYSQL_TYPE_INT24, "-8388608")
	assert.Equal(t, mediumValue.(int32), int32(-8388608))

	longValue := decoder.buildValue(MYSQL_TYPE_LONG, "2147483647")
	assert.Equal(t, longValue.(uint32), uint32(2147483647))
	longValue = decoder.buildValue(MYSQL_TYPE_LONG, "0")
	assert.Equal(t, longValue.(uint32), uint32(0))
	longValue = decoder.buildValue(MYSQL_TYPE_LONG, "-2147483648")
	assert.Equal(t, longValue.(int32), int32(-2147483648))

	longlongValue := decoder.buildValue(MYSQL_TYPE_LONGLONG, "9223372036854775807")
	assert.Equal(t, longlongValue.(uint64), uint64(9223372036854775807))
	longlongValue = decoder.buildValue(MYSQL_TYPE_LONGLONG, "-9223372036854775808")
	assert.Equal(t, longlongValue.(int64), int64(-9223372036854775808))

	setValue := decoder.buildValue(MYSQL_TYPE_SET, "5")
	assert.Equal(t, setValue.(uint64), uint64(5))

	enumValue := decoder.buildValue(MYSQL_TYPE_BIT, "65535")
	assert.Equal(t, enumValue.(uint64), uint64(65535))

	bitValue := decoder.buildValue(MYSQL_TYPE_BIT, "18446744073709551615")
	assert.Equal(t, bitValue.(uint64), uint64(18446744073709551615))

	tsValue := decoder.buildValue(MYSQL_TYPE_TIMESTAMP, "1531810179")
	assert.Equal(t, tsValue, time.Unix(1531810179, 0))

	ts2Value := decoder.buildValue(MYSQL_TYPE_TIMESTAMP2, "1531810179010")
	assert.Equal(t, ts2Value, time.Unix(1531810179, 10*1000*1000))

	yearValue := decoder.buildValue(MYSQL_TYPE_YEAR, "1990")
	assert.Equal(t, yearValue.(uint32), uint32(1990))

	nullValue := decoder.buildValue(MYSQL_TYPE_NULL, "")
	assert.Equal(t, nullValue, nil)

}
