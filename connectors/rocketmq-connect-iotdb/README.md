##### IotdbSourceConnector fully-qualified name
org.apache.rocketmq.connect.iotdb.connector.IotdbSourceConnector

**iotdb add data**

For new iotdb data, please refer to [iotdb quick start](https://iotdb.apache.org/UserGuide/V1.0.x/QuickStart/QuickStart.html)

**iotdb-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/iotdbSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.iotdb.connector.IotdbSourceConnector",
    "iotdbHost":"localhost",
    "iotdbPort":6667,
    "iotdbPaths":"root.ln.wf01.wt01",
    "connect.topicname":"iotdb1",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```


##### parameter configuration

parameter | effect                                                                                                             | required |default
---|--------------------------------------------------------------------------------------------------------------------|----------| ---
iotdbHost | The Host of the iotdb server                                                                                       | yes      | null
iotdbPort | The Port of the iotdb server                                                                                       | yes      |  null
paths| The path of the device(entity),Multiple paths are separated by commas (,) e.g. root.ln.wf01.wt01,root.ln.wf01.wt02 | yes      | null