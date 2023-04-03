##### ActiveConnector fully-qualified name
org.apache.rocketmq.connect.rabbitmq.connector.RabbitmqSourceConnector

**rabbitmq-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/rabbitmqSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.rabbitmq.connector.RabbitmqSourceConnector",
    "max.tasks":"2",
    "host":"localhost",
    "port":5672,
    "username":"guest",
    "password":"guest",
    "destinationType":"topic",
    "destinationName":"testTopic",
    "connect.topicname":"rabbitmqTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

parameter | effect                                                                                                                                                    | required |default
---|-----------------------------------------------------------------------------------------------------------------------------------------------------------|--- | ---
host | The host of the RabbtiMQ broker                                                                                                                           | yes | null
port | The port of the RabbtiMQ broker                                                                                                                           | yes | null
username | The username to use when connecting to RabbtiMQ                                                                                                           | yes |  null
password| The password to use when connecting to RabbtiMQ                                                                                                           | yes  | null
destinationName | The name of the JMS destination (queue or topic) to read from                                                                                             |  yes | null
destinationType | The type of JMS destination, which is either queue or topic                                                                                               | yes | null
jms.session.acknowledge.mode | The acknowledgement mode for the JMS Session                                                                                                              | null | Session.AUTO_ACKNOWLEDGE
jms.session.transacted | Flag to determine if the session is transacted and the session completely controls. the message delivery by either committing or rolling back the session | null | false

