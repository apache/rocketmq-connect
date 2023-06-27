##### OSSSinkConnector fully-qualified name

org.apache.rocketmq.connect.oss.sink.OSSSinkConnector

**oss-sink-connector** start

```shell
POST  http://${runtime-ip}:${runtime-port}/connectors/OSSSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.oss.sink.OSSSinkConnector",
    "endpoint":"https://oss-cn-hangzhou.aliyuncs.com",
    "accessKeyId":"<yourAccessKeyId>",
    "accessKeySecret":"<yourAccessKeySecret>",
    "bucketName ":"examplebucket",
    "max.tasks":2,
    "connect.topicnames":"test-oss-topic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter          | effect                                         | required        | default |
| ------------------ | ---------------------------------------------- | --------------- | ------- |
| endpoint           | endpoint to connect to                         | yes             | null    |
| accessKeyId        | access key id                                  | yes             | null    |
| accessKeySecret    | access key secret                              | yes             | null    |
| bucketName         | bucket name to connect to                      | yes             | null    |
| connect.topicnames | RocketMQ topic for sink connector to read from | yes (sink only) | null    |
