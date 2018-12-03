# DRC问题排查思路

数据对账工具如果发现数据不一致，排查问题从以下几个方面入手：
1. 查看failed_trx接口，看是否有执行出错的事务。
例如：http://10.10.10.1:9595/api/v1/applier/failed_trx
2. 根据数据对账工具找到对应的主键，在applier日志中查看是否该记录出现过冲突。
3. 如果出现了冲突，可以从源头开始，通过mysqlbinlog工具查看源数据的binlog数据，找到该记录。
4. 查看kafka中rdp如何处理该记录。

总体就是从整个数据流:mysql->rdp-->kafka-->applier-->mysql上来逐步排查问题。
