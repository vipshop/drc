# 关于DRC
DRC(Data Replication Center)是唯品会自研的MySQL双向复制方案，主要应用场景是数据库双向复制、单向复制。

DRC在[实时数据管道(RDP)](https://github.com/vipshop/RDP)的基础上，实现了以下功能：
* 支持MySQL实例的数据双向复制。
* 规避双向复制导致的循环复制。
* 检测异常情况下导致的数据冲突，并能自动处理, 保证两端的数据`最终一致`。
* 支持多线程重放binlog数据(MTS算法), 保证复制的吞吐量。
* 支持数据对账, 检验双向复制正确性。

# 总体架构
DRC总体架构如下所示：

![drc_architecture](/docs/picture/drc_architecture2.png)

图中，S0和S1是DB数据分片。

经过api_router流量分发后，写分片S0的流量由IDC1负责，写分片S1的流量由IDC2负责。
IDC1与IDC2之间的数据通过两个DRC实例进行同步。

> 数据分片可以对应不同的子表，也可以是对应同一个表中的不同范围。



单个DRC实例的架构如下所示：

![drc_instance](/docs/picture/drc_instance.png)

DRC实例包含三个组件：
* RDP：实时拉取源DB的binlog，解析之后的事务数据写入到Kafka。
* Kafka：存储解析之后的事务数据。
* Applier：订阅Kafka中的事务数据，通过执行SQL语句的方式，重放到目标DB。

# 关于Applier
Applier是DRC中的主要组件，订阅来自RDP解析后的binlog数据，构造SQL语句后在目标MySQL执行。

Applier的主要功能有：SQL构造, 并行执行，冲突检测与处理，过滤循环复制的数据，中断恢复，重复数据幂等处理。


# 限制条件
* MySQL版本必须在5.7.19及以上(因为MySQL 5.7.19之前的MTS算法存在BUG)。
* MySQL需要开启GTID, 并设置binlog_format=ROW, binlog_row_image=FULL。
* 业务数据库必须有主键字段(Applier处理数据冲突时依赖记录的主键)。


# 快速开始
## 1. 安装
### 源码安装
在源码根目录执行make，即可在build/mysql_applier/bin目录生成Applier可执行文件，同时产生实例配置文件：build/mysql-applier/etc/applier.ini.example。
```
[apps@localhost drc]$ make
[apps@localhost drc]$ ls build/mysql-applier/bin/
alarm.sh mysql-applier
[apps@localhost drc]$ ls build/mysql-applier/etc/
applier.ini.example

```


## 2. 修改配置
执行以下命令，修改Applier依赖的配置文件。

```
cp build/mysql-applier/etc && cp applier.ini.example applier.ini
vi applier.ini 
```

修改Kafka连接信息, 包括：brokerlist、订阅topic、partition和version。
```
[kafka]
#The kafka address of applier to fetch binlog event
brokerlist = 10.10.10.1:9092,10.10.10.2:9092,10.10.10.3:9092
topic = test_topic_1
partition = 0
version = 1.1.0
```
> 该Kafka topic中的数据是由RDP生成的，可以认为是源DB的binlog流。

修改目标DB连接信息。
```
[mysql]
#The mysql address of applier to write
host = localhost
port = 3306
user = root
passwd = 123456
```

> 注意：目标DB需要是源DB的历史镜像，后续通过Applier重放源DB的增量binlog。

> 注意：目标DB的所需最低权限是：SELECT , INSERT, UPDATE, DELETE, CREATE, DROP, ALTER,  REFERENCES, INDEX 

修改Zookeeper连接信息,包括：Zookeeper地址、zk节点路径（多个Applier副本依赖zk进行选主）。
```
[zk]
zk_addr_list = 10.10.10.1:2181,10.10.10.2:2181,10.10.10.3:2181
zk_root = /drc/mysql_applier/1000x
```

修改冲突处理方式，这里选择overwrite，表示如果复制过程中出现冲突，直接覆盖目标DB的记录。

当然，也可以选择time_overwrite, 表示基于时间戳来进行冲突处理，但是前提是表结构需要有特定的时间字段。

```
#The strategy to handle conflict, may be time_ignore|time_overwrite|ignore|overwrite
#if handle conflict base on time,need set the column name of row update time
handle_conflict_strategy=overwrite
update_time_column = update_time
```
> 对于线上使用，需要审慎选择冲突处理的方式，如果是双向复制，必须两个方向的处理方式不同：比如一边是overwrite，另一边是ignore。

## 3. 启动
假设配置文件build/mysql-applier/etc/applier.ini已经修改完成，那么执行以下命令启动Applier：

```
mkdir -p build/mysql_applier/logs
mkdir -p build/mysql_applier/metrics
cd build/mysql_applier/bin
./mysql-applier 
```
可以通过查看build/mysql_applier/logs目录下的日志文件，检查是否有ERROR信息。
如果有ERROR信息，请检查配置文件是否配置妥当。

## 4. 测试
在源DB执行DML或者DDL，检查是否在目标DB重放成功。

> 上述是单向复制搭建的步骤，双向复制搭建也是类似但更复杂些，建议用户根据自身需求，打造自动化搭建流程。

# 性能
关于DRC Applier的性能，请查看文档: [DRC性能概览](./docs/drc_perf.md)。

# 监控特性
关于DRC Applier的暴露的监控特性，请查看文档: [监控特性](./docs/applier_metrics.md)。

# 异常告警
关于DRC Applier运行过程中的异常告警，请查看文档：[异常告警](./docs/alarm_introduce.md)。

# 管理API
关于DRC Applier的提供的http管理API，请查看文档: [管理API](./docs/applier_api.md)。

# 数据对账
DRC提供了对账工具，用于比对两端数据是否一致，请查看文档: [数据对账](./docs/table_checksum.md)。

# 开发团队
DRC项目由唯品会基础架构部—数据中间件组开发和维护。开发成员如下：
- [陈非](https://github.com/flike)
- [赵百忠](https://github.com/firnsan)
- [汤锦平](https://github.com/tom-tangjp)
- [范力彪](https://github.com/libiaofan)

# License
DRC项目遵循Apache 2.0 license。
