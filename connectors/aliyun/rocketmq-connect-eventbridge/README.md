# rocketmq-connect-eventbridge
* **rocketmq-connect-eventbridge** 说明
```
Be responsible for consuming messages from producer and writing data to eventbridge.
```

## rocketmq-connect-eventbridge 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-eventbridge 启动

* **eventbridge-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-eventbridge-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.eventbridge.sink.EventBridgeSinkConnector",“connect-topicname”:"${connect-topicname}"
,accessKeyId”:"${accessKeyId}",accessKeySecret”:"${accessKeySecret}",accountEndpoint”:"${accountEndpoint}","eventId":"${eventId}","eventSource":"${eventSource}","eventType":"${eventType}", 
"eventTime":"${eventTime}","eventSubject":"${eventSubject}","aliyuneventbusname":"${aliyuneventbusname}"}
```

例子 
```
http://localhost:8081/connectors/eventbridgeConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.eventbridge.sink.EventBridgeSinkConnector",“connect-topicname”:"eventbridge-topic",accessKeyId”:"xxxx",accessKeySecret”:"xxxx",accountEndpoint”:"xxxx","eventId":"xxxx",
"eventSource":"xxxx","eventType":"", "eventTime":"xxxx","eventSubject":"", "aliyuneventbusname":"xxxx"}
```

>**注：** `rocketmq-eventbridge-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-eventbridge 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-eventbridge-connector-name}/stop
```

## rocketmq-connect-eventbridge 参数说明
* **eventbridge-sink-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description                      | Example
|------------------------|---------|----------------|----------------------------------|--|
|accessKeyId             | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                    | xxxx |
|accessKeySecret         | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                     | xxx |
|accountEndpoint         | String  | YES            | 阿里云EventBridge官方接入点                     | xxxx |
|eventId                 | String  | YES            | 事件ID | xxxx |
|eventSource             | String  | YES            | 事件源 | xxxx |
|eventType               | String | YES             | 事件类型                           | null |
|eventTime               | String | YES             | 事件产生的时间                          | xxxx |
|eventSubject            | String | NO              | 事件主题                          | xxxx |
|aliyuneventbusname      | String | YES             | 接收事件的事件总线名称                          | xxxx |
|connect-topicname       | String  | YES            | sink需要处理数据消息topic                     | xxxx |

