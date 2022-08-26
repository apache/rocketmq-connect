```
{
    "connector.class":"org.apache.rocketmq.connect.debezium.mysql.DebeziumMysqlConnector",
    "max.tasks":"1",
    "connect.topicname":"debezium-mysql-source",
    "kafka.transforms": "Unwrap",
    "kafka.transforms.Unwrap.delete.handling.mode": "none",
    "kafka.transforms.Unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "kafka.transforms.Unwrap.add.headers": "op,source.db,source.table",
    "database.history.name.srv.addr": "localhost:9876",
    "database.history.rocketmq.topic": "db-history-debezium-topic",
    "database.history.store.only.monitored.tables.ddl": true,
    "include.schema.changes": false,
    "database.server.name": "server-1",
    "database.port": 3306,
    "database.hostname": “localhost”,
    "database.user": “*******”,
    "database.password": “**********”,
    "database.connectionTimeZone": "Asia/Shanghai",
    "table.include.list": "testdb.testTable",
    "max.batch.size": 5,
    "database.include.list": "testdb",
    "snapshot.mode": "when_needed",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}

```