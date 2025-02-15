**rocketmq-connect-hologres**

在启动runtime之后，通过发送http消息到runtime，携带connector和task的参数，启动connector

**参数说明**

- **connector-class**: connector的类名
- **tasks.num**: 启动的task数目

##### parameter configuration

| parameter | effect                          | required | default |
|-----------|---------------------------------|----------|---------|
| host      | The Host of the hologres server | yes      | null    |
| port      | The Port of the hologres server | yes      | null    |
| database  | The info of the database        | yes      | null    |
| table     | The info of the table           | yes      | null    |
| username  | The info of the username        | yes      | null    |
| password  | The info of the password        | yes      | null    |

**启动 Source Connector**
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/hologresSourceConnector -d '{"connector.class":"org.apache.rocketmq.connect.hologres.connector.HologresSourceConnector","host":"localhost","port":"80","database":"test_db","username":"","password":"","table":"public.test_table","connect.topicname":"holoTopic","max.tasks":"2","slotName":"","startTime":"2023-05-31 12:00:00","value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter","key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"}'

```
POST  http://${runtime-ip}:${runtime-port}/connectors/hologresSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.hologres.connector.HologresSinkConnector",
    "host":"localhost",
    "port":80,
    "database":"test_db",
    "table":"public.test_table",
    "max.tasks":2,
    "connect.topicname":"holoTopic",
    "slotName":"",
    "startTime":"2023-05-31 12:00:00",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter                  | effect                                   | required | default |
|----------------------------|------------------------------------------|----------|---------|
| slotName                   | The slotName of the binlog               | yes      | null    |
| startTime                  | Where to start consume binlog            | yes      | null    |
| binlogReadBatchSize        | The read batch size of binlog reader     | no       | 1024    |
| binlogHeartBeatIntervalMs  | The heart beat interval of binlog reader | no       | -1      |
| binlogIgnoreDelete         | Whether ignore Delete operation          | no       | false   |
| binlogIgnoreBeforeUpdate   | Whether ignore BeforeUpdate operation    | no       | false   |
| retryCount                 | The max retry times of consume binlog    | no       | 3       |
| binlogCommitTimeIntervalMs | The commit interval of binlog            | no       | 5000    |

**启动Sink Connector**
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/hologresSinkConnector -d '{"connector.class":"org.apache.rocketmq.connect.hologres.connector.HologresSinkConnector","host":"localhost","port":"80","database":"test_db","username":"","password":"","table":"public.test_table","connect.topicnames":"holoTopic","max.tasks":"2","value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter","key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"}'

```
POST  http://${runtime-ip}:${runtime-port}/connectors/hologresSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.hologres.connector.HologresSinkConnector",
    "host":"localhost",
    "port":80,
    "database":"test_db",
    "table":"public.test_table",
    "max.tasks":2,
    "connect.topicnames":"holoTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter        | effect                             | required | default           |
|------------------|------------------------------------|----------|-------------------|
| dynamicPartition | Whether open dynamicPartition      | no       | false             |
| writeMode        | The write mode of the holo client  | no       | INSERT_OR_REPLACE |

**查看Connector运行状态**

http://127.0.0.1:8081/connectors/connector-name/status

**查看Connector配置**

http://127.0.0.1:8081/connectors/connector-name/config

**关闭Connector**

http://127.0.0.1:8081/connectors/connector-name/stop
