# MySQL-Applier Metrics说明
具体的metric如下所述：

* 	secondsBehindMaster，落后master的时间
* 	inChannelTrxCount，在管道中等待调度的事务数量
* 	fetchedTrxCount，接收到的事务数量
* 	filteredTrxCount，被过滤掉的事务数量
* 	applyedTrxCount，成功应用的事务数量
* 	deleteConflictCount，Delete冲突个数
* 	updateConflictCount，Update冲突个数
* 	insertConflictCount，Insert冲突个数
* 	applyTps，应用事务的tps
* 	scheduleTimeHistogram，调度耗时, 统计最近8192个样
* 	applyTimeHistogram，执行耗时
* 	totalTimeHistogram，从接收到执行成功总耗时