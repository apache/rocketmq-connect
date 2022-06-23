# rocketmq-connect-mns
* **rocketmq-connect-mns** 说明
```
It is responsible for obtaining data from the message service MNS and sending it to rocketmq through producer.
```

## rocketmq-connect-mns 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-mns 启动

* **mns-source-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-mns-source-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.mns.source.MNSSourceConnector","connect-topicname" : "${connect-topicname}",“accessKeyId”:"${accessKeyId}",accessKeySecret”:"${accessKeySecret}",accountEndpoint”:"${accountEndpoint}",queueName”:"${queueName}","accountId":"${accountId}","batchSize":"${batchSize}","isBase64Decode":"${isBase64Decode}"}
```

例子

```
http://localhost:8081/connectors/mnsConnectorSource?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.mns.source.MNSSourceConnector","connect-topicname" : "mns-topic","accessKeyId":"xxxx","accessKeySecret":"xxxx","accountEndpoint":"xxxx","queueName":"xxxx",
"accountId":"xxxx","batchSize":"8","isBase64Decode":"true"}
```

>**注：** `rocketmq-mns-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-mns 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-mns-connector-name}/stop
```

## rocketmq-connect-mns 参数说明
* **mns-source-connector 参数说明**

| KEY             | TYPE    | Must be filled | Description             | Example  
|-----------------|---------|----------------|-------------------------|----------|
| accessKeyId     | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取 | xxxx     |
| accessKeySecret | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取 | xxxx     |
| accountEndpoint | String  | YES            | 阿里云MNS官方接入点             | xxxx     |
| queueName       | String  | YES            | 队列名称                    | xxxx     |
| accountId       | String  | YES            | 阿里云yourAccountId        | 10000000 |
| batchSize       | Integer | NO            | 批量接受消息数量                | 8        |
| isBase64Decode  | String  | NO             | 是否开启Base64解码            | true     |
|connect-topicname       | String  | YES            | source需要处理数据消息topic     | xxxx |