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
	log "github.com/Sirupsen/logrus"
)

var (
	CAQ_UNDEF_INDEX = ^uint64(0)
)

type AssignedQueue struct {
	data []*Trx

	// 从avail端插入元素,也即是队列尾部
	avail uint64
	// 从entry端取出元素,也即是队列头部
	entry uint64
	len   uint64
	size  uint64

	// 最近一次计算的lwm
	lastLwm      int64
	lastLwmIndex uint64
	lastLwmTrx   *Trx

	// 回收队列空间后的lwm
	checkpointLwm    int64
	checkpointLwmTrx *Trx
}

func newAssignedQueue(size uint64) *AssignedQueue {
	return &AssignedQueue{
		size:  size,
		entry: size,
		data:  make([]*Trx, size, size),
	}
}

func (o *AssignedQueue) enQueue(trx *Trx) uint64 {

	if o.full() {
		return CAQ_UNDEF_INDEX
	}

	idx := o.avail
	o.data[o.avail] = trx

	// pre-boundary cond
	if o.entry == o.size {
		o.entry = o.avail
	}

	o.avail = (o.avail + 1) % o.size
	o.len++

	// post-boundary cond
	if o.avail == o.entry {
		o.avail = o.size
	}

	return idx
}

func (o *AssignedQueue) deQueue() (uint64, *Trx) {

	if o.empty() {
		return CAQ_UNDEF_INDEX, nil
	}

	idx := o.entry
	trx := o.data[o.entry]
	o.len--

	// pre boundary cond
	if o.avail == o.size {
		o.avail = o.entry
	}

	o.entry = (o.entry + 1) % o.size

	// post boundary cond
	if o.avail == o.entry {
		o.entry = o.size
	}

	return idx, trx
}

// 从队列头部清理所有已经commit的事务, 直到遇到未commit的事务,
// 返回最后一个已经commit的事务
func (o *AssignedQueue) moveQueueHead() *Trx {
	var lastTrx *Trx

	i := o.entry
	cnt := uint64(0)
	for i != o.avail && !o.empty() {
		trx := o.data[i]
		if trx.done == false {
			break
		}

		_, lastTrx = o.deQueue()
		o.checkpointLwm = trx.SequenceNumber
		o.checkpointLwmTrx = trx

		i = (i + 1) % o.size
		cnt++
	}

	return lastTrx

}

func (o *AssignedQueue) resetLwm() {
	o.lastLwm = 0
	o.checkpointLwm = 0
}

func (o *AssignedQueue) empty() bool {
	return o.entry == o.size
}

func (o *AssignedQueue) full() bool {
	return o.avail == o.size
}

func (o *AssignedQueue) in(k uint64) bool {
	if o.empty() {
		return false
	}
	if o.entry > o.avail {
		return (k >= o.entry || k < o.avail)
	} else {
		return (k >= o.entry && k < o.avail)
	}
}

// 从startIndex起始扫描出当前lwm
func (o *AssignedQueue) findLwm(startIdx uint64) (uint64, *Trx) {
	if o.empty() {
		return o.size, nil
	}

	// The startIdx must being in the running range: [entry, avail - 1].
	if !o.in(startIdx) {
		log.Panicf("AssignedQueue.findLwm: startIdx is invalid, startIdx: %d, entry: %d, avail: %d",
			startIdx, o.entry, o.avail)
	}

	var trx *Trx
	i := startIdx % o.size
	cnt := uint64(0)
	for cnt < o.len-(startIdx+o.size-o.entry)%o.size {
		trx = o.data[i]
		if trx.done == false {
			if cnt == 0 {
				// the first node of the queue is not done
				return o.size, nil
			}
			break
		}
		i = (i + 1) % o.size
		cnt++
	}

	idx := (i + o.size - 1) % o.size

	return idx, o.data[idx]
}

// 刷新lwm
func (o *AssignedQueue) refreshLwm() {
	isStale := false

	if isSeqLeq(o.lastLwm, o.checkpointLwm) {
		// 上一次计算的lwm已经被回收了
		isStale = true
	}

	if isStale {
		o.lastLwm = o.checkpointLwm
		o.lastLwmIndex = o.entry - 1
		o.lastLwmTrx = o.checkpointLwmTrx
	}

	startIndex := o.lastLwmIndex + 1

	lastLwmIndex, trx := o.findLwm(startIndex)
	if trx != nil {
		if trx.done == false {
			log.Panicf("AssignedQueue.refreshLwm: trx is not done")
		}
		o.lastLwm = trx.SequenceNumber
		o.lastLwmIndex = lastLwmIndex
		o.lastLwmTrx = trx
	}
}

// 获取上一次计算的lwm，也即是当前lwm的估计值
func (o *AssignedQueue) estimateLwm() int64 {
	return o.lastLwm
}

// 获取上一次计算的lwm对应的trx
func (o *AssignedQueue) estimateLwmTrx() *Trx {
	return o.lastLwmTrx
}

// 判断a是否小于等于b
func isSeqLeq(a int64, b int64) bool {
	if a == 0 {
		return true
	} else if b == 0 {
		return false
	} else {
		return a <= b
	}
}
