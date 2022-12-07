##### ActiveConnector完全限定名
org.apache.rocketmq.connect.activemq.connector.ActivemqConnector

**activemq-source-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/activeSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.activemq.connector.ActivemqSourceConnector",
    "max-task":"2",
    "activemqUrl":"tcp://localhost:61616",
    "destinationType":"queue",
    "destinationName":"testQueue",
    "connect.topicname":"targetTopic",
    "value-converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key-converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### 配置参数

参数 | 作用 | 是否必填 | 默认值
---|--- |--- | ---
activemqUrl | activemq ip与端口号 | 是 | 无
activemqUsername | 用户名 | 否 |  无
activemqPassword|  密码    | 否  | 无
destinationName | 读取的队列或者主题名   |  是 | 无
destinationType | 读取的类型：queue(队列)或者topic(主题) | 是 | 无
messageSelector | 过滤器    |  否  |无
sessionAcknowledgeMode | 消息确认  | 否 | Session.AUTO_ACKNOWLEDGE
sessionTransacted | 是否是事务会话      | 否 | false
