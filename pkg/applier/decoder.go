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
	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pierrec/lz4"
	"github.com/siddontang/go/hack"
	"github.com/vipshop/drc/pkg/applier/rdp_messages"
	"github.com/vipshop/drc/pkg/utils"
	"strconv"
	"strings"
	"time"
)

// 写入kafka数据
type RdpPkg struct {
	rdp_messages.KafkaPkg
	Topic  string
	Offset int64
}

type PkgSn struct {
	trxSeq   uint64
	splitSeq uint64
}

func (o *PkgSn) zero() bool {
	return o.trxSeq == 0 && o.splitSeq == 0
}

// 判断是否等于rhs
func (o *PkgSn) equal(rhs PkgSn) bool {
	return o.trxSeq == rhs.trxSeq && o.splitSeq == rhs.splitSeq
}

// 判断是否小于rhs
func (o *PkgSn) lt(rhs PkgSn) bool {
	if o.trxSeq < rhs.trxSeq {
		return true
	}
	if o.trxSeq == rhs.trxSeq && o.splitSeq < rhs.splitSeq {
		return true
	}
	return false
}

func (o *PkgSn) incTrxSeq() {
	o.trxSeq++
	o.splitSeq = 0
}

func (o *PkgSn) incSplitSeq() {
	o.splitSeq++
}

func (o *PkgSn) resetSplitSeq() {
	o.splitSeq = 0
}

type ReorderWindow struct {
	capacity   int
	pkg        *RdpPkg
	epoch      uint64
	expectedSn PkgSn
	// 连续遇到的乱序pkg数量
	incontinuousRdpPkg int
	// 乱序pkg数量累计一定程度后的告警阈值
	incontinuousRdpPkgThreshold int
}

func newReorderWindow(capacity int, incontinuousRdpPkgThreshold int) *ReorderWindow {
	return &ReorderWindow{
		capacity:                    capacity,
		incontinuousRdpPkgThreshold: incontinuousRdpPkgThreshold,
	}
}

func (o *ReorderWindow) ignoreDuplicatedRdpPkg(pkg *RdpPkg) {
	log.WithFields(log.Fields{
		"epoch":      pkg.GetEpoch(),
		"trx_seq":    pkg.GetTransSeqNo(),
		"split_seq":  pkg.GetSeqNo(),
		"split_flag": pkg.GetSplitFlag(),
		"gtid":       pkg.GetGtid(),
	}).Warnf("ReorderWindow.ignoreDuplicatedRdpPkg: ignore rdp pkg")
}

func (o *ReorderWindow) ignoreIncontinuousRdpPkg(pkg *RdpPkg) {
	log.WithFields(log.Fields{
		"epoch":      pkg.GetEpoch(),
		"trx_seq":    pkg.GetTransSeqNo(),
		"split_seq":  pkg.GetSeqNo(),
		"split_flag": pkg.GetSplitFlag(),
		"gtid":       pkg.GetGtid(),
	}).Warnf("ReorderWindow.ignoreIncontinuousRdpPkg: ignore rdp pkg")
	o.incontinuousRdpPkg++
	if o.incontinuousRdpPkg%o.incontinuousRdpPkgThreshold == 0 {
		// 乱序的包已经累计超过一定数量，需要告警
		msg := fmt.Sprintf("ignored incontinuous to much, ignored count: %d", o.incontinuousRdpPkg)
		utils.Alarm(msg)
	}
}

func (o *ReorderWindow) feed(pkg *RdpPkg) error {
	thisPkgSn := PkgSn{
		trxSeq:   pkg.GetTransSeqNo(),
		splitSeq: pkg.GetSeqNo(),
	}

	if o.epoch == 0 {
		// 第一次进入本方法时，需要初始化
		o.epoch = pkg.GetEpoch()
		o.expectedSn = thisPkgSn
	}

	if pkg.GetEpoch() < o.epoch {
		// 忽略掉RDP旧leader的数据
		log.Warnf("ReorderWindow.feed: meet a smaller epoch : %d, current epoch %d", pkg.GetEpoch(), o.epoch)
		o.ignoreDuplicatedRdpPkg(pkg)
		return nil
	}

	if pkg.GetEpoch() > o.epoch {
		// 这是rdp新leader产生的数据,
		// 预期的sn的trxSeq不变，但是split为0
		o.expectedSn.resetSplitSeq()
	}

	o.epoch = pkg.GetEpoch()

	if thisPkgSn.lt(o.expectedSn) {
		// thisPkgSn 小于 expectedSn, 说明是重复出现的trxSeq和splitSeq
		// 也即是同一个rdp进程产生的重复pkg, 因为kafka保证aleast once, 所以可能有重复, 需要忽略掉
		// 根据trxSeq去重的前提条件是：
		//		1. 同一个epoch的trxSeq是连续递增的，如果kafka中同一个trxSeq出现多次，较后出现的需要忽略
		//		2. 不同epoch的trxSeq也是连续递增的（不考虑复杂的双leader存在的话，我们假设这个条件满足），同上
		log.Warnf("ReorderWindow.feed: meet a smaller seq number: %d.%d, expected: %d.%d",
			thisPkgSn.trxSeq, thisPkgSn.splitSeq, o.expectedSn.trxSeq, o.expectedSn.splitSeq)
		o.ignoreDuplicatedRdpPkg(pkg)
		return nil
	}

	if !thisPkgSn.equal(o.expectedSn) {
		// 出现了不连续的trxSeq和splitSeq, 需要报错
		// 前提条件也与上述前提条件相同
		log.Warnf("ReorderWindow.feed: meet a incontinuous seq number: %d.%d, expected: %d.%d",
			thisPkgSn.trxSeq, thisPkgSn.splitSeq, o.expectedSn.trxSeq, o.expectedSn.splitSeq)
		o.ignoreIncontinuousRdpPkg(pkg)
		return nil
	}

	if pkg.GetSplitFlag() != 1 {
		// 该pkg对应的事务已经集齐, 预期sn的trxSeq++
		o.expectedSn.incTrxSeq()
	} else {
		// 该pkg对应的事务还没有集齐，预期sn的splitSeq++
		o.expectedSn.incSplitSeq()
	}

	o.incontinuousRdpPkg = 0
	o.pkg = pkg

	return nil
}

// 取出顺序的下一个pkg
func (o *ReorderWindow) peekNext() *RdpPkg {
	pkg := o.pkg
	o.pkg = nil
	return pkg
}

type RdpDecoder struct {
	reorderWindow *ReorderWindow
	// 缓存当前事务的分包
	pkgs []*RdpPkg

	// 标记是否有可以组装的事务
	wholeReceived bool
}

func NewRdpDecoder(incontinuousRdpPkgThreshold int) *RdpDecoder {
	return &RdpDecoder{
		reorderWindow: newReorderWindow(1024*10, incontinuousRdpPkgThreshold),
	}
}

func (o *RdpDecoder) feed(message []byte, topic string, offset int64) error {
	var err error
	if o.wholeReceived == true {
		// 已经缓存了完整事务，需要先通过peek取出该事务
		return ErrNeedToPeekTrx
	}
	// 从kafka消息中解析出RdpPkg
	var pkg RdpPkg
	err = o.constructRdpPkg(message, topic, offset, &pkg)
	if err != nil {
		log.Errorf("RdpDecoder.feed: construct rdp pkg error: %s", err)
		utils.Alarm("decoder construct rdp pkg error: " + err.Error())
		return err
	}

	// 需要对到达的pkg进行排序
	err = o.reorderWindow.feed(&pkg)
	if err != nil {
		log.Errorf("RdpDecoder.feed: reorder window feed error: %s", err)
		utils.Alarm("decoder feed reorder window error: " + err.Error())
		return err
	}

	// 取出已经顺序到达、已经去重的pkg
	// 在此处进行组装
	nextRdpPkg := o.reorderWindow.peekNext()
	for ; nextRdpPkg != nil; nextRdpPkg = o.reorderWindow.peekNext() {
		if len(o.pkgs) != 0 {
			if nextRdpPkg.GetEpoch() != o.pkgs[0].GetEpoch() {
				// 如果该pkg的epoch已经改变, 需要清空已经缓存的pkg
				o.pkgs = nil
			}
		}
		o.pkgs = append(o.pkgs, nextRdpPkg)
		if nextRdpPkg.GetSplitFlag() != 1 {
			// 当前事务的分包已经收集完成
			o.wholeReceived = true
		}
	}

	return nil

}

func (o *RdpDecoder) canPeek() bool {
	return o.wholeReceived
}

// 取出组装之后的完整Trx
func (o *RdpDecoder) peek() (*Trx, error) {
	var err error
	var finalTrx Trx // 组装之后的trx

	err = o.decodeRdpPkg(o.pkgs[0], &finalTrx)
	if err != nil {
		return nil, err
	}
	finalTrx.Topic = o.pkgs[0].Topic
	finalTrx.BeginOffset = o.pkgs[0].Offset

	for _, pkg := range o.pkgs[1:] {
		var trx Trx
		err = o.decodeRdpPkg(pkg, &trx)
		if err != nil {
			return nil, err
		}
		// 将该trx组装到finalTrx
		o.mergeTrx(&finalTrx, &trx)
	}
	finalTrx.EndOffset = o.pkgs[len(o.pkgs)-1].Offset

	o.wholeReceived = false
	o.pkgs = nil

	return &finalTrx, nil

}

func (o *RdpDecoder) decodeRdpPkg(pkg *RdpPkg, trx *Trx) error {
	var err error
	// 判断是否需要解压
	var needDecompress = false
	var data []byte
	if pkg.GetFlag() != 0 &&
		(pkg.GetFlag()&uint32(rdp_messages.KafkaPkgFlag_kKfkPkgCompressData) == uint32(rdp_messages.KafkaPkgFlag_kKfkPkgCompressData)) {
		needDecompress = true
	}
	if needDecompress {
		data, err = o.decompress(pkg.GetData(), pkg.GetSourceDataLen())
		if err != nil {
			log.Errorf("RdpDecoder.unmarshalKafkaMessage: decompress error: %s", err)
			return err
		}
	} else {
		data = pkg.GetData()
	}

	// 解码出Transaction
	transaction := rdp_messages.Transaction{}
	err = proto.Unmarshal(data, &transaction)
	if err != nil {
		log.Errorf("RdpDecoder.unmarshalKafkaMessage: unmarshal Transaction error: %s", err)
		return err
	}

	o.constructTrx(&transaction, trx)

	return nil
}

func (o *RdpDecoder) constructColumn(column *rdp_messages.Column, c *Column) {
	if column.BinlogType == nil {
		log.Panic("RdpDecoder.constructColumn: binlog_type field not found in pb")
	}
	c.Name = hack.String(column.GetName())
	c.Type = hack.String(column.GetType())
	c.BinlogType = ColumnType(column.GetBinlogType())
	c.IsNull = (column.GetIsNull() == 1)

	if c.IsNull {
		c.Value = nil
	} else {
		// 根据字段类型，将string转换成相应类型
		c.Value = o.buildValue(c.BinlogType, hack.String(column.GetValue()))
	}

	keyType := column.GetKey()
	if keyType == "PRI" || keyType == "UNI" {
		// 如果该字段是主键或者是unique key，则认为是主键之一
		c.IsPk = true
	}
}

func (o *RdpDecoder) constructRow(row *rdp_messages.Row, r *Row) {
	for _, column := range row.GetBefore() {
		var c Column
		o.constructColumn(column, &c)
		r.Before = append(r.Before, &c)
	}
	for _, column := range row.GetAfter() {
		var c Column
		o.constructColumn(column, &c)
		r.After = append(r.After, &c)
	}
}

func (o *RdpDecoder) constructEvent(event *rdp_messages.Event, e *Event) {

	e.DatabaseName = hack.String(event.GetDatabaseName())
	e.TableName = hack.String(event.GetTableName())
	e.EventType = EventType(event.GetEventType())
	e.Timestamp = event.GetTimestamp()
	e.TimestampOfReceipt = event.GetTimestampOfReceipt()
	e.Position = event.GetPosition()
	e.NextPosition = event.GetNextPosition()
	e.BinlogFileName = hack.String(event.GetBinlogFileName())
	e.ServerId = event.GetServerId()
	e.SqlStatement = hack.String(event.GetSqlStatement())
	for _, row := range event.GetRows() {
		var r Row
		o.constructRow(row, &r)
		e.Rows = append(e.Rows, &r)
	}
}

func (o *RdpDecoder) constructTrx(transaction *rdp_messages.Transaction, trx *Trx) {
	trx.Gtid = hack.String(transaction.GetGtid())
	trx.Seq = uint64(transaction.GetSeq())
	trx.Position = transaction.GetPosition()
	trx.BinlogFileName = hack.String(transaction.GetBinlogFileName())
	trx.NextPosition = transaction.GetNextPosition()
	trx.NextBinlogFileName = hack.String(transaction.GetNextBinlogFileName())
	trx.LastCommitted = transaction.GetLastCommitted()
	trx.SequenceNumber = transaction.GetSequenceNumber()

	trx.fetchedTime = time.Now()

	for _, event := range transaction.GetEvents() {
		eventType := EventType(event.GetEventType())
		if eventType == GTID_LOG_EVENT {
			// 以GTID的时间作为该事务的时间
			trx.Timestamp = event.GetTimestamp()
		}

		if eventType != WRITE_ROWS_EVENT && eventType != UPDATE_ROWS_EVENT &&
			eventType != DELETE_ROWS_EVENT && eventType != QUERY_EVENT {
			// 只需要ROW和QUERY事件
			//log.Debugf("RdpDecoder.ConstructTrx: unknown event type: %d", eventType)
			continue
		}

		if eventType == QUERY_EVENT {
			statement := strings.ToUpper(hack.String(event.GetSqlStatement()))
			// If the query is not (BEGIN | XA START | COMMIT | [XA] ROLLBACK), it can be considered an ordinary statement.
			if strings.Compare(statement, "BEGIN") != 0 &&
				!strings.HasPrefix(statement, "XA START") &&
				!strings.HasPrefix(statement, "ROLLBACK") &&
				!strings.HasPrefix(statement, "ROLLBACK TO ") &&
				!strings.HasPrefix(statement, "XA ROLLBACK") &&
				strings.Compare(statement, "COMMIT") != 0 {
				// 如果不是这些语句，需要添加到events中
				var e Event
				o.constructEvent(event, &e)
				trx.Events = append(trx.Events, &e)
			} else {
				// 忽略掉BEGIN, COMMIT等QUERY_EVENT
				// log.Debugf("RdpDecoder.constructTrx: not ddl event: %s", statement)
			}
		} else {
			// 无需过滤WRITE_ROWS_EVENT,UPDATE_ROWS_EVENT,DELETE_ROWS_EVENT
			var e Event
			o.constructEvent(event, &e)
			trx.Events = append(trx.Events, &e)
		}
	}

}

func (o *RdpDecoder) mergeTrx(finalTrx *Trx, trx *Trx) {
	finalTrx.Events = append(finalTrx.Events, trx.Events...)
}

func (o *RdpDecoder) decompress(src []byte, len uint64) ([]byte, error) {
	var err error
	if len == 0 {
		log.Panicf("Fetcher.decompress: src data len is zero ")
	}
	dst := make([]byte, len)
	n := 0
	n, err = lz4.UncompressBlock(src, dst, 0)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil

}

func (o *RdpDecoder) parseInteger(s string, base int, bitSize int) (i interface{}, err error) {
	// 根据字符串是否以负号开始
	isUnsigned := true
	if s != "" && s[0] == '-' {
		isUnsigned = false
	}

	if isUnsigned {
		// 无符号类型
		i, err = strconv.ParseUint(s, base, bitSize)
	} else {
		i, err = strconv.ParseInt(s, base, bitSize)
	}
	if err != nil {
		return
	}

	if isUnsigned {
		v := i.(uint64)
		switch bitSize {
		case 0:
			i = uint(v)
		case 8:
			i = uint8(v)
		case 16:
			i = uint16(v)
		case 32:
			i = uint32(v)
		case 64:
			i = uint64(v)
		}
	} else {
		v := i.(int64)
		switch bitSize {
		case 0:
			i = int(v)
		case 8:
			i = int8(v)
		case 16:
			i = int16(v)
		case 32:
			i = int32(v)
		case 64:
			i = int64(v)
		}
	}
	return
}

func (o *RdpDecoder) buildValue(columnType ColumnType, columnValue string) interface{} {
	var arg interface{}

	switch columnType {
	case MYSQL_TYPE_TINY:
		v, err := o.parseInteger(columnValue, 10, 8)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_SHORT:
		v, err := o.parseInteger(columnValue, 10, 16)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_LONG:
		v, err := o.parseInteger(columnValue, 10, 32)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_LONGLONG:
		v, err := o.parseInteger(columnValue, 10, 64)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_INT24:
		v, err := o.parseInteger(columnValue, 10, 32)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_SET:
		//max size: 8 Byte
		v, err := o.parseInteger(columnValue, 10, 64)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_ENUM:
		//max sieze: 2 Byte
		v, err := o.parseInteger(columnValue, 10, 16)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v

	case MYSQL_TYPE_BIT:
		//max size: 8 Byte
		v, err := o.parseInteger(columnValue, 10, 64)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_TIMESTAMP:
		v, err := o.parseInteger(columnValue, 10, 64)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}

		if v.(uint64) == uint64(0) {
			// MySQL中timestamp为0表示的是非法的值，也即是0000-00-00 00:00:00
			// 不能转换成1907-01-01 00:00:00
			arg = "0000-00-00 00:00:00"
			break
		}
		// 旧timestamp的单位是s
		s := int64(v.(uint64))
		arg = time.Unix(s, 0)
	case MYSQL_TYPE_TIMESTAMP2:
		v, err := o.parseInteger(columnValue, 10, 64)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}

		if v.(uint64) == uint64(0) {
			arg = "0000-00-00 00:00:00"
			break
		}
		// 新timestamp的单位是ms
		ms := int64(v.(uint64))
		arg = time.Unix(ms/1000, ms%1000*int64(time.Millisecond))
	case MYSQL_TYPE_YEAR:
		v, err := o.parseInteger(columnValue, 10, 32)
		if err != nil {
			log.Fatalf("Fetcher.buildValue: ParseInt error: %s,value:%s", err.Error(), columnValue)
		}
		arg = v
	case MYSQL_TYPE_NULL:
		arg = nil
	default:
		arg = columnValue
	}
	return arg
}

func (o *RdpDecoder) constructRdpPkg(message []byte, topic string, offset int64, pkg *RdpPkg) error {
	var err error
	// 解码出vms结构
	var vmsPkg rdp_messages.VMSMessage
	err = proto.Unmarshal(message, &vmsPkg)
	if err != nil {
		log.Errorf("Fetcher.unmarshalKafkaMessage: unmarshal vmsPkg error: %s", err)
		return err
	}
	// 解码出rdp结构
	err = proto.Unmarshal(vmsPkg.GetPayload(), &pkg.KafkaPkg)
	if err != nil {
		log.Errorf("Fetcher.unmarshalKafkaMessage: unmarshal kafkaPkg error: %s", err)
		return err
	}
	pkg.Topic = topic
	pkg.Offset = offset

	return nil
}
