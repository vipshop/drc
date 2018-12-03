# DRC性能
## 前言
DRC的数据复制的性能，主要取决于Applier重放binlog的性能.

Applier性能有两方面：吞吐量和复制延迟。

吞吐量主要取决于:
* Applier与目标DB的网络延时: 与目标数据库的延时越大，Applier的吞吐量越小。
* 源DB负荷的并发度：如果源DB的产生的binlog不能并发重放，那么Applier无法发挥多线程优势。

复制延迟主要取决于：
* Applier吞吐量与源DB的吞吐量差距：如果Applier的吞吐量长期低于源DB, 会导致复制延迟越来越大。

## 测试数据
关于Applier性能的简单测试数据如下表所示：

| 用例   |        源DB压力          | Applier与目标DB 延时(ms) |Applier吞吐量(tps)| 
| ------ | ------------------------ | ------------------------ | ---------------- | 
| 测试1  | 16 threads，19200 tps    | 0.02                     |       8070       | 
| 测试2  |  8 threads，11700 tps    | 0.02                     |       5050       |
| 测试3  |  4 threads， 6870 tps    | 0.02                     |       2960       | 
| 测试4  |  2 threads， 4530 tps    | 0.02                     |       1950       |
| 测试5  |  1 thread ， 2113 tps    | 0.02                     |       1274       | 
| 测试6  | 16 threads，19200 tps    | 0.21                     |       4640       | 
| 测试7  | 16 threads，19200 tps    | 2.01                     |       1145       | 

> sysbench用例: update_index
> sysbench与源数据库网络延时: 0.04ms
> Applier机器配置: 32 core, 32G RAM
> Applier与kafka集群网络延时: 5ms
> DB机器配置：32core，32G RAM, SAS盘


# 性能内幕

从上表我们可以看出：
1. 随着与目标DB的rtt增大，Applier的性能会急剧下降，**建议与目标DB同机房部署**。
2. 随着DB负荷的并发度越低，Applier的性能会近似线性下降，吞吐量是sysbench的40%~55%。
3. 由于Applier和源DB的吞吐量一直有差距，随着时间推移，DRC的复制延迟会越来越大。


其中，我们解释一下，Applier的吞吐量与sysbench.update_index的吞吐量差距的原因，主要是：
1. DRC在双向同步的过程中，为了避免binlog无限循环复制，Applier对重放的事务进行特殊标记：额外更新了一个table, 也就是说Applier比sysbench.update_index多执行一条update语句。
2. Applier重放事务的过程中，auto_commit=0, 比sysbench.update_index多执行了begin、commit语句。

所以在100%写的情况下，Applier与源DB的吞吐量会有较大差距, 随着时间推移，DRC的复制延迟会越来越大。

但是对于读写混合负载，比如在sysbench.oltp压力测试下，Applier完全可以及时重放事务，不会造成主从延时。


