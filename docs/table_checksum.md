
# 数据对账
使用DRC进行数据复制的过程中，业务、运维需要定期验证DRC的正确性，也即是校验两个MySQL的数据是否一致。

我们有两个工具可以校验两个MySQL之间的数据是否一致，分别是：
* px-table-checksum, 开源的数据校验工具，存在**误报**的情况, 适合在没有更新流量的情况执行
* drc-table-checksum，DRC自研的数据校验工具, 不会存在误报的情况, 但是依赖数据库SUPER权限，**建议由DBA执行**。

下文会详细对比这两个工具，并介绍使用方法。



## px-table-checksum 介绍
px-table-checksum工具的源码路径：https://github.com/seanlook/px-table-checksum

### 工作原理
从源库批量（即chunk）取出一块数据（如1000行），计算CRC32值，同样的语句在目标库运行一遍，结果都存入第三方数据库，最后检查对应编号的chunk crc值是否一致。

### 优缺点
优点：对业务数据库的权限依赖低，只需要SELECT权限，无需SUPER、DML、DDL等权限。

缺点：
1. 存在误报的情况，也即是如果工具的检查结果是“数据有差异”，实际上可能是一致的，需要重新核对。
2. 依赖REDIS队列存放SQL，工具会从该队列取出SQL在目标库重放。

> 误报是因为在源库执行SQL、在目标库执行SQL存在一个时间差(比如是1s)，在这个时间段内，目标库的数据可能有改变（比如目标库apply了源库的binlog），导致两次计算的CRC不一致。


### 使用说明

#### 1. 配置说明
在settings_cs_tables.py中，配置了源DB和目标DB的信息，如下所示：
```

db_host_src = '10.10.10.1'
db_host_dst = '10.10.10.2'

TABLES_CHECK = {"test": ["table1"]}

DB_SOURCE = {'db_host': db_host_src,
           'db_port': 10000,
           'db_user': 'root',
           'db_charset': 'utf8mb4',
           'result_charset': 'utf-8'}

DB_TARGET = {'db_host': db_host_dst,
           'db_port': 30000,
           'db_user': 'root',
           'db_charset': 'utf8mb4',
           'result_charset': 'utf-8'}
```

在settings_checksum.py中，配置了REDIS队列的信息和checksum结果数据存放位置:
```
# sql队列 redis地址
REDIS_INFO = "10.10.10.1:6379"

# checksum结果数据存放位置
DB_CHECKSUM = {'db_host': '10.10.10.1',
           'db_port': 10000,
           'db_user': 'root',
           'db_charset': 'utf8',
           'db_name': 'px_checksum'}
```


#### 2.启动命令

```
 python ./px-table-checksum.py 
```


## drc-table-checksum 介绍

### 工作原理
通过特定的SQL语句，在源库计算一块数据（如1000行）的CRC32值，存入源库的某个table中，并通过statement格式的主从复制在目标库运行一遍，存入目标库的一个table中。 最后检查对应编号的chunk crc值是否一致。

### 优缺点
优点: 不存在误报的情况，因为已经通过主从复制（也就是DRC）来在目标库执行SQL语句。

缺点：依赖在业务库SUPER、SELECT权限，而且需要在业务库新建一个schema以及table，需要SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX 权限。

> 因为drc-table-checksum需要设置数据库的binlog_format（SESSION级别）变量，需要SUPER权限。
> 但是SUPER权限是风险比较大的一项权限，所以建议由DBA来执行drc-table-checksum。

### 使用说明

drc-table-checksum 是DRC的数据对账工具，位于源码的:tool/目录下。
drc-table-checksum实现原理借鉴了pt-table-checksum，通过在Master上计算每个chunk的crc32，然后再Slave上计算相同chunk的crc32，
生成的crc32信息会单独写入一张表（checksum表），最后通过检查checksum表中Master、Slave上相同chunk的crc32是否一致，来确定两个MySQL实例数据是否一致。
drc-table-checksum包含两个文件：drc-table-checksum.py和settings_cs_tables.py。

#### 1. 配置说明
在settings_cs_tables.py中，配置了源DB和目标DB的信息，如下所示：

```
SRC_CONN_INFO = {'db_host': '10.10.10.1',
           'db_port': 3415,
           'db_user': 'root',
           'db_pass': '123456',
           'db_charset': 'utf8mb4'}

DEST_CONN_INFO = {'db_host': '10.10.10.2',
           'db_port': 3415,
           'db_user': 'root',
           'db_pass': '123456',
           'db_charset': 'utf8mb4'}
```
SRC_CONN_INFO对应的DB实例必须是允许DDL的DRC的上游MySQL实例。在一个双向复制的MySQL实例对中，只有一个DRC允许执行DDL。

#### 2.启动命令

 ```
 python ./drc-table-checksum.py --target_table=sysbench.sbtest1 --checksum_table=drc.checksum --chunk_size=100 --lower_boundary=-1 --upper_boundary=-1
 ```
*  target_table:待检查的表，包含db名和表名。
*  checksum_table:生成的checksum表，包含db名和表名。
*  chunk_size:一次检查chunk的大小
*  lower_boundary:检查范围的下限，-1表示从表的最小偏移开始检查。
*  upper_boundary:检查范围的上限，-1表示检查到表的最大偏移。

