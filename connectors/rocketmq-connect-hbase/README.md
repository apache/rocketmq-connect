
##### HBaseSinkConnector fully-qualified name
org.apache.rocketmq.connect.hbase.sink.HBaseSinkConnector

**hbase-sink-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/HBaseSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.hbase.sink.HBaseSinkConnector",
    "zkquorum":"localhost:2181",
    "columnfamily":"cf",
    "username":"default",
    "password":"123456",
    "connect.topicnames":"testHBaseTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter          | effect                                         | required        | default |
|--------------------|------------------------------------------------|-----------------|---------|
| zkquorum           | The Endpoint of the Zookeeper server           | yes             | null    |
| columnfamily       | The Column Family of the Destination table     | yes             | null    |
| username           | The UserName to login to HBase server          | yes             | null    |
| password           | The Password of the UserName                   | no              | null    |
| hbasemaster        | The Endpoint of HBase Master server            | no              | null    |
| connect.topicnames | RocketMQ topic for sink connector to read from | yes (sink only) | null    |
