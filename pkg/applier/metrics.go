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
	"github.com/vipshop/drc/pkg/metrics"
)

var (
	// 落后master的时间
	secondsBehindMaster = metrics.NewGauge()

	// 在管道中等待调度的事务数量
	inChannelTrxCount = metrics.NewGauge()

	// 接收到的事务数量
	fetchedTrxCount = metrics.NewCounter()

	// 成功应用的事务数量
	applyedTrxCount = metrics.NewCounter()

	// Delete冲突个数
	deleteConflictCount = metrics.NewCounter()

	// Update冲突个数
	updateConflictCount = metrics.NewCounter()

	// Insert冲突个数
	insertConflictCount = metrics.NewCounter()

	// 应用事务的tps
	applyTps = metrics.NewMeter()

	// 执行事务的并发度
	applyConcurrency = metrics.NewGauge()

	// 调度耗时, 统计最近8192个样本
	scheduleTimeHistogram = metrics.NewHistogram(8192)

	// 执行耗时
	applyTimeHistogram = metrics.NewHistogram(8192)

	// 从接收到执行成功总耗时
	totalTimeHistogram = metrics.NewHistogram(8192)

	// 事务的大小
	trxSizeHistogram = metrics.NewHistogram(8192)
)

func init() {

	metrics.Register("seconds_behind_master", secondsBehindMaster)

	metrics.Register("in_channel_trx_count", inChannelTrxCount)

	metrics.Register("fetched_trx_count", fetchedTrxCount)

	metrics.Register("applyed_trx_count", applyedTrxCount)

	metrics.Register("delete_conflict_count", deleteConflictCount)
	metrics.Register("update_conflict_count", updateConflictCount)
	metrics.Register("insert_conflict_count", insertConflictCount)

	metrics.Register("apply_tps", applyTps)
	metrics.Register("apply_concurrency", applyConcurrency)

	metrics.Register("schedule_time_histogram", scheduleTimeHistogram)
	metrics.Register("apply_time_histogram", applyTimeHistogram)
	metrics.Register("total_time_histogram", totalTimeHistogram)

	metrics.Register("trx_size_histogram", trxSizeHistogram)

}
