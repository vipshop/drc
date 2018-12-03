# MySQL-Aplier告警说明

## 1.告警概述

Applier内部运行过程中如果遭遇异常，会触发告警。告警信息通过alarm.sh脚本发送到指定的平台。
用户可以根据自身情况，修改alarm.sh脚本，将告警信息发送到适当的地方。

## 2.重要告警

下面列举一些Applier重要的告警关键字。

### 2.1 decoder feed reorder window error

问题及原因：kafka中的消息编号不连续，可能是kafka中的数据发生丢失、乱序。

处理措施：无需处理，等待rdp自愈，重新发送正确的数据，整个异常期间不会造成Applier重放事务的丢失或者乱序。



### 2.2 exec trx error

问题及原因：Applier执行事务失败。

处理措施：通过API接口，定位到原因（可能是因为两边DB的一些配置不一致）之后，重试该事务。



