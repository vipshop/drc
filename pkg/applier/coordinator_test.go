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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type Action struct {
	duration time.Duration
	err      error
}

var (
	gActions map[string]Action
	//
	gExecedC chan *Trx
	// 实际运行顺序
	gActualOrder []*Trx
)

func mockExec(ctx context.Context, opts *ExecOpt, trx *Trx) {
	var err error
	execResult := ExecResult{
		Err: nil,
		Trx: trx,
	}

	defer func() {
		if err != nil {
			execResult.Err = err
		}
		opts.resultC <- execResult
	}()

	if isIllegal(opts, trx) {
		err = ErrIllegalTrx
		return
	}

	action, ok := gActions[trx.Gtid]
	if !ok {
		panic("unknown gtid")
	}

	time.Sleep(action.duration)
	err = action.err
	if err == nil {
		gExecedC <- trx
	}
}

func waitAllTrxExeced(totalCount int, quitC chan<- struct{}) {
	defer close(quitC)
	count := 0
	for {
		trx := <-gExecedC
		gActualOrder = append(gActualOrder, trx)
		count++
		if count == totalCount {
			// 已经等到了所有事务
			return
		}
	}
}

// 测试不同group的事务串行执行
func TestSerialExec(t *testing.T) {
	gActions = make(map[string]Action)
	gExecedC = make(chan *Trx)
	gActualOrder = make([]*Trx, 0)

	trxC := make(chan *Trx)
	coordinator := NewCoordinator(&ApplierConfig{}, trxC, nil, nil)
	coordinator.execFunc = mockExec
	ctx, _ := context.WithCancel(context.Background())
	go coordinator.run(ctx)

	trx0 := Trx{
		BeginOffset:    0,
		EndOffset:      0,
		Gtid:           "0",
		LastCommitted:  0,
		SequenceNumber: 1,
	}
	gActions["0"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	trx1 := Trx{
		BeginOffset:    1,
		EndOffset:      1,
		Gtid:           "1",
		LastCommitted:  1,
		SequenceNumber: 2,
	}
	gActions["1"] = Action{
		duration: 3 * time.Second,
		err:      nil,
	}

	trx2 := Trx{
		BeginOffset:    2,
		EndOffset:      2,
		Gtid:           "2",
		LastCommitted:  2,
		SequenceNumber: 3,
	}
	gActions["2"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	quitC := make(chan struct{})
	go waitAllTrxExeced(len(gActions), quitC)

	trxC <- &trx0
	trxC <- &trx1
	trxC <- &trx2
	<-quitC

	expectedOrder := []*Trx{&trx0, &trx1, &trx2}
	require.Equal(t, expectedOrder, gActualOrder)
}

// 测试同一group的事务并发执行
func TestSameGroupParallExec(t *testing.T) {
	gActions = make(map[string]Action)
	gExecedC = make(chan *Trx)
	gActualOrder = make([]*Trx, 0)

	trxC := make(chan *Trx)
	coordinator := NewCoordinator(&ApplierConfig{}, trxC, nil, nil)
	coordinator.execFunc = mockExec
	ctx, _ := context.WithCancel(context.Background())
	go coordinator.run(ctx)

	trx0 := Trx{
		BeginOffset:    0,
		EndOffset:      0,
		Gtid:           "0",
		LastCommitted:  0,
		SequenceNumber: 1,
	}
	gActions["0"] = Action{
		duration: 1 * time.Second,
		err:      nil,
	}

	// trx1执行时间较trx2长
	trx1 := Trx{
		BeginOffset:    1,
		EndOffset:      1,
		Gtid:           "1",
		LastCommitted:  1,
		SequenceNumber: 2,
	}
	gActions["1"] = Action{
		duration: 3 * time.Second,
		err:      nil,
	}

	trx2 := Trx{
		BeginOffset:    2,
		EndOffset:      2,
		Gtid:           "2",
		LastCommitted:  1,
		SequenceNumber: 3,
	}
	gActions["2"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	trx3 := Trx{
		BeginOffset:    3,
		EndOffset:      3,
		Gtid:           "3",
		LastCommitted:  3,
		SequenceNumber: 4,
	}
	gActions["3"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	quitC := make(chan struct{})
	go waitAllTrxExeced(len(gActions), quitC)

	trxC <- &trx0
	trxC <- &trx1
	trxC <- &trx2
	trxC <- &trx3
	<-quitC

	expectedOrder := []*Trx{&trx0, &trx2, &trx1, &trx3}
	require.Equal(t, expectedOrder, gActualOrder)
}

// 测试不同group的事务并发执行
func TestCrossGroupParallExec(t *testing.T) {
	gActions = make(map[string]Action)
	gExecedC = make(chan *Trx)
	gActualOrder = make([]*Trx, 0)

	trxC := make(chan *Trx)
	coordinator := NewCoordinator(&ApplierConfig{}, trxC, nil, nil)
	coordinator.execFunc = mockExec
	ctx, _ := context.WithCancel(context.Background())
	go coordinator.run(ctx)

	trx0 := Trx{
		BeginOffset:    0,
		EndOffset:      0,
		Gtid:           "0",
		LastCommitted:  0,
		SequenceNumber: 1,
	}
	gActions["0"] = Action{
		duration: 1 * time.Second,
		err:      nil,
	}

	trx1 := Trx{
		BeginOffset:    1,
		EndOffset:      1,
		Gtid:           "1",
		LastCommitted:  1,
		SequenceNumber: 2,
	}
	gActions["1"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	// trx2执行时间较trx3长
	trx2 := Trx{
		BeginOffset:    2,
		EndOffset:      2,
		Gtid:           "2",
		LastCommitted:  1,
		SequenceNumber: 3,
	}
	gActions["2"] = Action{
		duration: 3 * time.Second,
		err:      nil,
	}

	// trx3应该较trx2先完成
	trx3 := Trx{
		BeginOffset:    3,
		EndOffset:      3,
		Gtid:           "3",
		LastCommitted:  2,
		SequenceNumber: 4,
	}
	gActions["3"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	quitC := make(chan struct{})
	go waitAllTrxExeced(len(gActions), quitC)

	trxC <- &trx0
	trxC <- &trx1
	trxC <- &trx2
	trxC <- &trx3
	<-quitC

	expectedOrder := []*Trx{&trx0, &trx1, &trx3, &trx2}
	require.Equal(t, expectedOrder, gActualOrder)
}

// 对于binlog名字不同的的trx，串行执行
func TestCrossBinlogSerialExec(t *testing.T) {
	gActions = make(map[string]Action)
	gExecedC = make(chan *Trx)
	gActualOrder = make([]*Trx, 0)

	trxC := make(chan *Trx)
	coordinator := NewCoordinator(&ApplierConfig{}, trxC, nil, nil)
	coordinator.execFunc = mockExec
	ctx, _ := context.WithCancel(context.Background())
	go coordinator.run(ctx)

	trx0 := Trx{
		BeginOffset:    0,
		Gtid:           "0",
		BinlogFileName: "0",
		LastCommitted:  0,
	}
	gActions["0"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	trx1 := Trx{
		BeginOffset:    1,
		EndOffset:      1,
		Gtid:           "1",
		BinlogFileName: "1",
		LastCommitted:  0,
	}
	gActions["1"] = Action{
		duration: 3 * time.Second,
		err:      nil,
	}

	trx2 := Trx{
		BeginOffset:    2,
		EndOffset:      2,
		Gtid:           "2",
		BinlogFileName: "2",
		LastCommitted:  0,
	}
	gActions["2"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	trx3 := Trx{
		BeginOffset:    3,
		EndOffset:      3,
		Gtid:           "3",
		BinlogFileName: "3",
		LastCommitted:  3,
	}
	gActions["3"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	quitC := make(chan struct{})
	go waitAllTrxExeced(len(gActions), quitC)

	trxC <- &trx0
	trxC <- &trx1
	trxC <- &trx2
	trxC <- &trx3
	<-quitC

	expectedOrder := []*Trx{&trx0, &trx1, &trx2, &trx3}
	require.Equal(t, expectedOrder, gActualOrder)
}

// 测试trx失败并触发重试
func TestExecFailed(t *testing.T) {
	gActions = make(map[string]Action)
	gExecedC = make(chan *Trx)
	gActualOrder = make([]*Trx, 0)

	trxC := make(chan *Trx)
	retryC := make(chan string)
	coordinator := NewCoordinator(&ApplierConfig{}, trxC, retryC, nil)
	coordinator.execFunc = mockExec
	ctx, _ := context.WithCancel(context.Background())
	go coordinator.run(ctx)

	trx0 := Trx{
		BeginOffset:    0,
		EndOffset:      0,
		Gtid:           "0",
		LastCommitted:  0,
		SequenceNumber: 1,
	}
	gActions["0"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	trx1 := Trx{
		BeginOffset:    1,
		EndOffset:      1,
		Gtid:           "1",
		LastCommitted:  1,
		SequenceNumber: 2,
	}
	gActions["1"] = Action{
		duration: time.Microsecond,
		err:      errors.New("exec sql failed"),
	}

	trx2 := Trx{
		BeginOffset:    2,
		EndOffset:      2,
		Gtid:           "2",
		LastCommitted:  1,
		SequenceNumber: 3,
	}
	gActions["2"] = Action{
		duration: time.Second,
		err:      nil,
	}

	trx3 := Trx{
		BeginOffset:    3,
		EndOffset:      3,
		Gtid:           "3",
		LastCommitted:  3,
		SequenceNumber: 4,
	}
	gActions["3"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	quitC := make(chan struct{})
	go waitAllTrxExeced(len(gActions), quitC)

	trxC <- &trx0
	trxC <- &trx1
	trxC <- &trx2
	trxC <- &trx3

	time.Sleep(3 * time.Second)
	// trx1应该是已经失败
	waitToHandle := coordinator.getWaitToHandle()
	_, ok := waitToHandle[trx1.Gtid]
	require.True(t, ok)
	require.Equal(t, trx3.Gtid, coordinator.getPendingTrx().Gtid)

	// 重试trx1
	gActions["1"] = Action{
		duration: time.Microsecond,
		err:      nil,
	}
	retryC <- trx1.Gtid
	<-quitC

	// trx1应该排在trx2之后
	expectedOrder := []*Trx{&trx0, &trx2, &trx1, &trx3}
	require.Equal(t, expectedOrder, gActualOrder)

}

// 测试trx非法并触发跳过
func TestExecIllegal(t *testing.T) {
	gActions = make(map[string]Action)
	gExecedC = make(chan *Trx)
	gActualOrder = make([]*Trx, 0)

	trxC := make(chan *Trx)
	skipC := make(chan string)
	coordinator := NewCoordinator(&ApplierConfig{AllowDDL: false}, trxC, nil, skipC)
	coordinator.execFunc = mockExec
	ctx, _ := context.WithCancel(context.Background())
	go coordinator.run(ctx)

	trx0 := Trx{
		BeginOffset:    0,
		EndOffset:      0,
		Gtid:           "0",
		LastCommitted:  0,
		SequenceNumber: 1,
	}
	gActions["0"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	// trx1是非法的事务
	event := &Event{
		SqlStatement: "drop table a",
	}
	trx1 := Trx{
		BeginOffset:    1,
		EndOffset:      1,
		Gtid:           "1",
		LastCommitted:  0,
		SequenceNumber: 2,
		Events:         []*Event{event},
	}

	trx2 := Trx{
		BeginOffset:    2,
		EndOffset:      2,
		Gtid:           "2",
		LastCommitted:  0,
		SequenceNumber: 3,
	}
	gActions["2"] = Action{
		duration: time.Second,
		err:      nil,
	}

	trx3 := Trx{
		BeginOffset:    3,
		EndOffset:      3,
		Gtid:           "3",
		LastCommitted:  3,
		SequenceNumber: 4,
	}
	gActions["3"] = Action{
		duration: time.Millisecond,
		err:      nil,
	}

	quitC := make(chan struct{})
	go waitAllTrxExeced(len(gActions), quitC)

	trxC <- &trx0
	trxC <- &trx1
	trxC <- &trx2
	trxC <- &trx3

	time.Sleep(3 * time.Second)
	// trx1应该是已经失败
	waitToHandle := coordinator.getWaitToHandle()
	_, ok := waitToHandle[trx1.Gtid]
	require.True(t, ok)
	require.Equal(t, trx3.Gtid, coordinator.getPendingTrx().Gtid)

	// 跳过trx1
	skipC <- trx1.Gtid
	<-quitC

	// trx1应该没有被执行
	expectedOrder := []*Trx{&trx0, &trx2, &trx3}
	require.Equal(t, expectedOrder, gActualOrder)

}
