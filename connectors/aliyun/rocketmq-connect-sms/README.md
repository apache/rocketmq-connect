# rocketmq-connect-sms
* **rocketmq-connect-sms** 说明
```
Responsible for consuming messages from producers and sending data via SMS.
```

## rocketmq-connect-sms 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-sms 启动

* **sms-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-sms-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.sms.sink.SmsSinkConnector",“accountEndpoint”:"${accountEndpoint}",accessKeyId”:"${accessKeyId}",accessSecretKey”:"${accessSecretKey}",phoneNumbers”:"${phoneNumbers}","signName":"${signName}","templateCode":"${templateCode}"}
```

例子 
```
http://localhost:8081/connectors/smsConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.sms.sink.SmsSinkConnector",“accountEndpoint”:"xxxx", "accessKeyId”:"xxxx", "accessSecretKey”:"xxxx", "phoneNumbers”:"xxxx","signName":"xxxx","templateCode":"xxxx"}
```

>**注：** `rocketmq-sms-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-sms 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-sms-connector-name}/stop
```

## rocketmq-connect-sms 参数说明
* **sms-sink-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description                      | Example
|------------------------|---------|----------------|----------------------------------|--|
|accountEndpoint         | String  | YES            | 访问的域名                               | xxxx |
|accessKeyId             | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                    | xxxx |
|accessKeySecret         | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                     | xxx |
|phoneNumbers            | String  | YES            | 接收短信的手机号码                     | xxxx |
|signName                | String  | YES            | 短信签名名称                     | xxxx |
|templateCode            | String  | YES            | 短信模板CODE                     | xxxx |

