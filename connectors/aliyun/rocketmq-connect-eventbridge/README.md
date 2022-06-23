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
,"stsEndpoint”:"${stsEndpoint}",accessKeyId”:"${accessKeyId}",accessKeySecret”:"${accessKeySecret}",roleArn”:"${roleArn}", "roleSessionName":"${roleSessionName}", "eventSubject":"${eventSubject}","aliyuneventbusname":"${aliyuneventbusname}"}
```

例子 
```
http://localhost:8081/connectors/eventbridgeConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.eventbridge.sink.EventBridgeSinkConnector",“connect-topicname”:"eventbridge-topic","stsEndpoint”:"xxxx",accessKeyId”:"xxxx",accessKeySecret”:"xxxx",
roleArn”:"xxxx", "roleSessionName":"xxxx", "aliyuneventbusname":"xxxx","eventSubject":""}
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
|stsEndpoint             | String  | NO              | STS endpoint                       | xxxx|
|accountEndpoint         | String  | YES              | 阿里云授信账号 endpoint                       | xxxx|
|accessKeyId             | String  | YES             | 阿里云授信账号的AK                    | xxxx |
|accessKeySecret         | String  | YES             | 阿里云授信账号的SK                     | xxx |
|roleArn                 | String  | NO             | 要扮演的RAM角色ARN。 该角色是可信实体为阿里云账号类型的RAM角色                     | xxxx |
|roleSessionName         | String  | NO             | 角色会话名称。 该参数为用户自定义参数。通常设置为调用该API的用户身份                  | 用户名 |
|eventSubject            | String  | YES             | 事件主题                          | xxxx |
|aliyuneventbusname      | String  | YES             | 接收事件的事件总线名称                          | xxxx |
|connect-topicname       | String  | YES             | sink需要处理数据消息topic                     | xxxx |

