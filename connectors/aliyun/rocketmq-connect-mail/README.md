# rocketmq-connect-mail
* **rocketmq-connect-mail** 说明
```
Responsible for consuming messages from producers and sending data via email.
```

## rocketmq-connect-mail 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-mail 启动

* **mail-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-mail-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.mail.sink.MailSinkConnector","connect-topicname" : "${connect-topicname}",“accountEndpoint”:"${accountEndpoint}", “accessKeyId”:"${accessKeyId}",accessKeySecret”:"${accessKeySecret}",accountName”:"${accountName}","addressType":"${addressType}","replyToAddress":"${replyToAddress}","toAddress":"${toAddress}", "subject":"${subject}", "isHtmlBody":"${isHtmlBody}"}
```

例子
```
http://localhost:8081/connectors/mailConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.mail.sink.MailSinkConnector","connect-topicname" : "xxxx",“accountEndpoint”:"xxxx",accessKeyId”:"xxxx",accessKeySecret”:"xxxx",accountName”:"xxxx","addressType":"xxxx","replyToAddress":"xxxx","toAddress":"xxxx", "subject":"xxxx","isHtmlBody":"true"}
```

>**注：** `rocketmq-mail-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-mail 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-mail-connector-name}/stop
```

## rocketmq-connect-mail 参数说明
* **mail-sink-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description                      | Example
|------------------------|---------|----------------|----------------------------------|--|
|accountEndpoint         | String  | YES            | 访问的域名                               | xxxx |
|accessKeyId             | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                    | xxxx |
|accessKeySecret         | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                     | xxx |
|accountName             | String  | YES            | 管理控制台中配置的发信地址                     | xxxx |
|addressType             | String  | YES            | 地址类型                     | 0 |
|replyToAddress          | String  | YES            | 使用管理控制台中配置的回信地址                     | true |
|toAddress               | String  | YES            | 目标地址                     | xxxx |
|subject                 | String  | YES            | 邮件主题                     | xxxx |
|isHtmlBody              | String  | YES            | 是否HTML正文                     | true |
|connect-topicname       | String  | YES            | sink需要处理数据消息topics                     | xxxx |

