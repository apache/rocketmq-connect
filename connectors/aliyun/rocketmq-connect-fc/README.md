# rocketmq-connect-fc
* **rocketmq-connect-fc** 说明
```
Be responsible for consuming messages from producer and writing data to function calculation FC.
```

## rocketmq-connect-fc 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-fc 启动

* **fc-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-fc-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.fc.sink.FcSinkConnector","connect-topicname" : "${connect-topicname}",“region”:"${region}",accessKey”:"${accessKey}",accessSecretKey”:"${accessSecretKey}",accountId”:"${accountId}","serviceName":"${serviceName}","functionName":"${functionName}","invocationType":"${invocationType}", "qualifier":"${qualifier}"}
```

例子 
```
http://localhost:8081/connectors/fcConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.fc.sink.FcSinkConnector","connect-topicname" : "fc-topic",“region”:"cn-hangzhou",accessKey”:"xxxx",accessSecretKey”:"xxxx",accountId”:"xxxx","serviceName":"xxxx","functionName":"xxxx","invocationType":"", "qualifier":"LATEST"}
```

>**注：** `rocketmq-fc-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-fc 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-fc-connector-name}/stop
```

## rocketmq-connect-fc 参数说明
* **fc-sink-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description                      | Example
|------------------------|---------|----------------|----------------------------------|--|
|region                  | String  | YES            | 地域                               | cn-hangzhou|
|accessKey               | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                    | xxxx |
|accessSecretKey         | String  | YES            | 阿里云身份验证，在阿里云用户信息管理控制台获取                     | xxx |
|accountId               | String  | YES            | 阿里云yourAccountId                      | xxxx |
|serviceName             | String  | YES            | 服务名称 | xxxx |
|functionName            | String  | YES            | 函数名称 | xxxx |
|invocationType          | String | NO             | 同步或者异步                           | null |
|qualifier               | String | NO             | 服务版本和别名                          | LATEST |
|connect-topicname       | String  | YES            | sink需要处理数据消息topic                     | xxxx |

