# rocketmq-connect-http
* **rocketmq-connect-http** 说明
```
Be responsible for consuming messages from producer and writing data to another web service system.
```

## rocketmq-connect-http 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-http 启动

* **http-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-http-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.http.sink.HttpSinkConnector","connect-topicname" : "${connect-topicname}","url":"${url}"}
```

例子 
```
http://localhost:8081/connectors/httpConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.http.sink.HttpSinkConnector","connect-topicname" : "http-topic","url":"192.168.1.2"}
```

>**注：** `rocketmq-http-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-http 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-http-connector-name}/stop
```

## rocketmq-connect-http 参数说明
* **http-sink-connector 参数说明**

| KEY |  TYPE   | Must be filled | Description | Example          
|-----|---------|----------------|-------------|------------------|
| url | String  | YES            | sink端 域名地址  | http://127.0.0.1 |
|connect-topicname       | String  | YES            | sink需要处理数据消息topic                     | xxxx |

