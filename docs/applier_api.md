# MySQL-Applier API接口说明
## 1.获取执行失败的事务
```
- Method: **GET**
- URL:  http://ip:port/api/v1/applier/failed_trx
- Headers：
- Body:
- Response:
{
  "message": "success",
  "data": {
  }
}

```
## 2.跳过失败事务
```
- Method: **PUT**
- URL: http://ip:port/api/v1/applier/failed_trx/skip
- Headers： Content-Type:application/json
- Body:
{
    "gtid" : "47fbb51c-266d-11e8-a07f-244427b6b60e:37922928"
}
- Response:
{
  "message": "success",
  "data": {
        "47fbb51c-266d-11e8-a07f-244427b6b60e:37922928"
  }
}

example: curl -v -X PUT --data '{"gtid":"47fbb51c-266d-11e8-a07f-244427b6b60e:37922928"}' http://127.0.0.1:9595/api/v1/applier/failed_trx/skip -H 'Content-Type: application/json'

```
## 3.重试失败事务
```
- Method: **PUT**
- URL: http://ip:port/api/v1/applier/failed_trx/retry
- Headers： Content-Type:application/json
- Body:
{
    "gtid" : "47fbb51c-266d-11e8-a07f-244427b6b60e:37922928"
}
- Response:
{
  "message": "success",
  "data": {
        "47fbb51c-266d-11e8-a07f-244427b6b60e:37922928"
  }
}
example: curl -v -X PUT --data '{"gtid":"47fbb51c-266d-11e8-a07f-244427b6b60e:37922928"}' http://127.0.0.1:9595/api/v1/applier/failed_trx/retry -H 'Content-Type: application/json'

```

## 4.查看Applier状态

```
- Method: **GET**
- URL:  http://ip:port/api/v1/applier/progress
- Headers：
- Body:
- Response:
{
  "message": "success",
  "data": {
    "kafka_offset": -1,
    "gtid": "",
    "binlog_file_position": 0,
    "binlog_file_name": ""
  }
}
```

## 5.查看Applier角色

```
- Method: **GET**
- URL:  http://ip:port/api/v1/applier/work_mode
- Headers：
- Body:
- Response:
{
  "message": "success",
  "data": "leader"
}

```
