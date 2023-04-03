##### ActiveConnector fully-qualified name
org.apache.rocketmq.connect.activemq.connector.ActivemqConnector

**activemq-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/activeSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.activemq.connector.ActivemqSourceConnector",
    "max-task":"3",
    "activemqUrl":"tcp://localhost:61616",
    "destinationType":"queue",
    "destinationName":"testQueue",
    "connect.topicname":"targetTopic",
    "value-converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key-converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

parameter | effect | required |default
---|--- |--- | ---
activemqUrl | The URL of the ActiveMQ broker | yes | null
activemqUsername | The username to use when connecting to ActiveMQ | no |  null
activemqPassword|  The password to use when connecting to ActiveMQ    | no  | null
destinationName | The name of the JMS destination (queue or topic) to read from   |  yes | null
destinationType | The type of JMS destination, which is either queue or topic | yes | null
messageSelector | The message selector that should be applied to messages in the destination    |  no  | null 
sessionAcknowledgeMode | The acknowledgement mode for the JMS Session  | null | Session.AUTO_ACKNOWLEDGE
sessionTransacted | Flag to determine if the session is transacted and the session completely controls. the message delivery by either committing or rolling back the session      | null | false
