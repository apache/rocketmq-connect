## FIXME: fix document

##### ${dbNameToCamel}SourceConnector fully-qualified name

org.apache.rocketmq.connect.${dbNameToLowerCase}.source.${dbNameToCamel}SourceConnector

**${dbNameToLowerCase}-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${dbNameToLowerCase}SourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.${dbNameToLowerCase}.source.${dbNameToCamel}SourceConnector",
    "${dbNameToLowerCase}host":"localhost",
    "${dbNameToLowerCase}port":8123,
    "database":"default",
    "username":"default",
    "password":"123456",
    "table":"tableName",
    "topic":"test${dbNameToCamel}Topic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### ${dbNameToCamel}SinkConnector fully-qualified name

org.apache.rocketmq.connect.${dbNameToLowerCase}.sink.${dbNameToCamel}SinkConnector

**${dbNameToLowerCase}-sink-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${dbNameToLowerCase}SinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.${dbNameToLowerCase}.sink.${dbNameToCamel}SinkConnector",
    "${dbNameToLowerCase}host":"localhost",
    "${dbNameToLowerCase}port":8123,
    "database":"clickhouse",
    "username":"default",
    "password":"123456",
    "connect.topicnames":"test${dbNameToCamel}Topic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter                | effect                                            | required          | default |
|--------------------------|---------------------------------------------------|-------------------|---------|
| ${dbNameToLowerCase}host | The Host of the ${dbNameToCamel} server           | yes               | null    |
| ${dbNameToLowerCase}port | The Port of the ${dbNameToCamel} server           | yes               | null    |
| database                 | The database to read or write                     | yes               | null    |
| table                    | The source table to read                          | yes (source only) | null    |
| topic                    | RocketMQ topic for source connector to write into | yes (source only) | null    |
| connect.topicnames       | RocketMQ topic for sink connector to read from    | yes (sink only)   | null    |
