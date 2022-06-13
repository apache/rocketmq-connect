# rocketmq-connect-rocketmq

## rocketmq-connect-rocketmq 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-rocketmq 启动

* **rocketmq-source-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-rocketmq-source-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.rocketmq.RocketMQSourceConnector","connect-topicname" : "${connect-topicname}",“accessKeyId”:"${accessKeyId}",accessKeySecret”:"${accessKeySecret}",namesrvAddr”:"${namesrvAddr}","topic":"${topic}","instanceId":"${instanceId}","consumerGroup":"${consumerGroup}"}
```

例子

```
http://localhost:8081/connectors/rocketmqConnectorSource?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.rocketmq.RocketMQSourceConnector","connect-topicname" : "rocketmq-source-topic","accessKeyId":"xxxx","accessKeySecret":"xxxx","namesrvAddr":"http://127.0.0.1:9876","topic":"topic",
"instanceId":"xxxx", "consumerGroup":"xxxx"}
```

* **rocketmq-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-rocketmq-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.rocketmq.RocketMQSinkConnector","connect-topicname" : "${connect-topicname}", "accessKeyId":"${accessKeyId}", "accessKeySecret":"${accessKeySecret}",namesrvAddr”:"${namesrvAddr}","topic":"${topic}","instanceId":"${instanceId}"}
```

例子 
```
http://localhost:8081/connectors/rocketmqConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.rocketmq.RocketMQSinkConnector","connect-topicname" : "rocketmq-sink-topic","accessKeyId":"xxxx","accessKeySecret":"xxxx","namesrvAddr":"http://127.0.0.1:9876","topic":"topic",
"instanceId":"xxxx"}
```

>**注：** `rocketmq-rocketmq-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-rocketmq 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-rocketmq-connector-name}/stop
```

## rocketmq-connect-rocketmq 参数说明
* **rocketmq-source-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description                                | Example
|------------------------|---------|----------------|--------------------------------------------|---|
| accessKeyId           | String  | YES            | AccessKey ID阿里云身份验证，在阿里云服务器管理控制台创建         | xxxx    |
| accessKeySecret       | String  | YES            | AccessKey Secret阿里云身份验证，在阿里云服务器管理控制台创建     | xxxx    |
| namesrvAddr           | String  | YES            | 设置TCP接入域名，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看 | xxxx    |
| topic                 | String  | YES            | 消息主题                                       | xxxx    |
| instanceId            | String  | NO             | 阿里云MQ控制台的实例Id                              | xxxx    |
| consumerGroup            | String  | YES            | 消息订阅者                                      | xxxx    |
|connect-topicname       | String  | YES            | source需要处理数据消息topic                        | xxxx |

```  
注：1. source/sink配置文件说明是以rocketmq-connect-rocketmq为demo，不同source/sink connector配置有差异，请以具体sourc/sink connector为准
```  
* **rocketmq-sink-connector 参数说明**

| KEY                   |  TYPE   | Must be filled | Description | Example 
|-----------------------|---------|----------------|-----------|---------|
| accessKeyId           | String  | YES            | AccessKey ID阿里云身份验证，在阿里云服务器管理控制台创建 | xxxx    |
| accessKeySecret       | String  | YES            | AccessKey Secret阿里云身份验证，在阿里云服务器管理控制台创建 | xxxx    |
| namesrvAddr           | String  | YES            | 设置TCP接入域名，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看 | xxxx    |
| topic                 | String  | YES            | 消息主题          | xxxx    |
| instanceId            | String  | NO             | 阿里云MQ控制台的实例Id | xxxx    |
|connect-topicname       | String  | YES            | sink需要处理数据消息topic                     | xxxx |

