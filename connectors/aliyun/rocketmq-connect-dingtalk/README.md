# rocketmq-connect-ding-talk

## rocketmq-connect-ding-talk 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-ding-talk 启动

* **ding-talk-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-ding-talk-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"com.aliyun.rocketmq.connect.dingtalk.sink.DingTalkSinkConnector",“webHook”:"${webHook}",msgtype”:"${msgtype}"}
```

例子 
```
http://localhost:8081/connectors/dingTalkConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"com.aliyun.rocketmq.connect.dingtalk.sink.DingTalkSinkConnector","webHook":"192.168.1.2","msgtype":"text"}
```

>**注：** `rocketmq-ding-talk-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-ding-talk 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-connector-name}/stop
```

## rocketmq-connect-ding-talk 参数说明
* **ding-talk-sink-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description   | Example  
|------------------------|---------|----------------|---------------|----------|
|webHook                   | String  | YES            | 机器人的Webhook地址 | https://oapi.dingtalk.com/robot/send?access_token=XXXXXX |
|msgtype                  | String  | NO             | 消息类型 | text     |

