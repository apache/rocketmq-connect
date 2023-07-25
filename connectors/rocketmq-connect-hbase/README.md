
##### HBaseSinkConnector fully-qualified name
org.apache.rocketmq.connect.hbase.sink.HBaseSinkConnector

**hbase-sink-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/clickhouseSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.clickhouse.sink.ClickHouseSinkConnector",
    "clickhousehost":"localhost",
    "clickhouseport":8123,
    "database":"clickhouse",
    "username":"default",
    "password":"123456",
    "connect.topicnames":"testClickHouseTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter          | effect                                            | required          | default |
|--------------------|---------------------------------------------------|-------------------|---------|
| clickhousehost     | The Host of the Clickhouse server                 | yes               | null    |
| clickhouseport     | The Port of the Clickhouse server                 | yes               | null    |
| database           | The database to read or write                     | yes               | null    |
| table              | The source table to read                          | yes (source only) | null    |
| topic              | RocketMQ topic for source connector to write into | yes (source only) | null    |
| connect.topicnames | RocketMQ topic for sink connector to read from    | yes (sink only)   | null    |
