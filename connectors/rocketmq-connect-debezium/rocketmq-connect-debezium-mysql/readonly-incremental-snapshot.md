# Incremental Snapshots

## 1.参考
- DBLog
> https://arxiv.org/pdf/2010.12597v1.pdf
- Incremental Snapshots
> https://debezium.io/blog/2021/10/07/incremental-snapshots/
- Read-only Incremental Snapshots
> https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/


## 问题 
当前debezium实现的默认全量快照功能，在全量过程中会出现锁表的现象，可能会导致以下问题
>1. 导致在使用过程中无法对表进行写入，这样快照时间只能避开用户使用期间，无法随时随地对表进行快照 
>2. 数据量大了可能造成锁表时间超时，快照失败
>3. 全量快照未保留上次快照的位点，一旦失败会造成重新全量快照，无法增量；

## Read-only Incremental Snapshots(mysql)

### 先决条件

此方案只适用于Mysql , 并依赖全局事务标志GTID，因此，如果你从只读副本读取，则需要设置 gtid_mode=on , 以保留GTID的顺序
```
gtid_mode = ON
enforce_gtid_consistency = ON
if replica_parallel_workers > 0 set replica_preserve_commit_order = ON
```
该算法运行SHOW MASTER STATUS查询以获取在块选择之前和之后设置的已执行 GTID：
```
low watermark = executed_gtid_set
high watermark = executed_gtid_set - low watermark
```

## 2.任务设置
```
{
    "connector.class":"org.apache.rocketmq.connect.debezium.mysql.DebeziumMysqlConnector",
    "max.tasks":"1",
    "connect.topicname":"debezium-mysql-source-0002",
    "kafka.transforms": "Unwrap",
    "kafka.transforms.Unwrap.delete.handling.mode": "none",
    "kafka.transforms.Unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "kafka.transforms.Unwrap.add.headers": "op,source.db,source.table",
    "database.history.name.srv.addr": "localhost:9876",
    "database.history.rocketmq.topic": "db-history-debezium-topic-0002",
    "database.history.store.only.monitored.tables.ddl": true,
    "database.serverTimezone":"UTC",
    "database.user": "bizworks",
    "include.schema.changes": false,
    "database.server.name": "server-0002",
    "database.port": 3306,
    "database.hostname": "localhost",
    "database.password": "******",
    "table.include.list": "test_database.employee_copy3",
    "max.batch.size": 5,
    "database.include.list": "test_database",
    "snapshot.mode": "schema_only",
    "read.only": "true",
    "incremental.snapshot.allow.schema.changes": "true",
    "incremental.snapshot.chunk.size": "5000",
    "signal.rocketmq.topic": "dbz-signals-001",
    "signal.name.srv.addr": "localhost:9876",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}

```
### 参数说明
- "read.only": "true",
> true 为开启 readonly 的增量快照模式
- snapshot.mode
> 增量快照下配置成schema_only，其它方式暂未测试，never会有空指针错误，后面修复 
- incremental.snapshot.allow.schema.changes
> 快照过程中是否允许schema 发生变化
- signal.rocketmq.topic
> 指定监听的信令的topic
- signal.name.srv.addr
> rocketmq name server addr

### 信令发送

```
sh mqadmin sendMessage -t dbz-signals-001 -n localhost:9876 -k server-0002 -p {"type":"execute-snapshot","data": {"data-collections": ["test_database.employee_copy3"], "type": "INCREMENTAL"}}
```
#### 信令发送配置

- rocketmq message key
> rocketmq 发送的消息key是 [database.server.name] 配置项
- rocketmq message value :
> 1. type： 固定值  "execute-snapshot"
> 2. data.data-collections ：为要监听的表的集合
> 3. data.type ：固定类型 "INCREMENTAL"
> 
```
{
    "type":"execute-snapshot",
    "data":{
        "data-collections":[
            "test_database.employee_copy3"
        ],
        "type":"INCREMENTAL"
    }
}
```