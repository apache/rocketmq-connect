##### HiveSourceConnector fully-qualified name
org.apache.rocketmq.connect.hive.connector.HiveSourceConnector

**hive-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/hiveSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.hive.connector.HiveSourceConnector",
    "host":"localhost",
    "port":10000,
    "database":"default",
    "tables":{
        "invites": {
            "foo":1
        }
    },
    "max.tasks":2,
    "connect.topicname":"hiveTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

parameter | effect                      | required |default
---|-----------------------------|----------| ---
host | The Host of the hive server | yes      | null
port | The Port of the hive server | yes      |  null
tables| The info of the table       | yes      | null
database | The info of the database | yes | null
username | The info of the hive server | no | null
password | The info of the hive server | no | null