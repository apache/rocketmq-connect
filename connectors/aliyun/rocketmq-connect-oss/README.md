# rocketmq-connect-oss
* **rocketmq-connect-oss** 说明
```
Be responsible for consuming messages from producer and writing data to oss.
```

## rocketmq-connect-oss 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-oss 启动

* **rocketmq-connect-oss** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-oss-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.oss.sink.OssSinkConnector","connect-topicname":"${connect-topicname}","accessKeyId":"${accessKeyId}","accessKeySecret":"${accessKeySecret}","accountEndpoint":"${accountEndpoint}","bucketName":"${bucketName}","fileUrlPrefix":"${fileUrlPrefix}","objectName":"${objectName}","region":"${region}","partitionMethod":"${partitionMethod}"}
```

例子 
```
http://localhost:8081/connectors/ossConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.oss.sink.OssSinkConnector","connect-topicname":"oss-topic","accountEndpoint":"xxxx","accessKeyId":"xxxx","accessKeySecret":"xxxx",
"bucketName":"xxxx","objectName":"xxxx","region":"xxxx","partitionMethod":"xxxx"}
```

>**注：** `rocketmq-oss-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-oss 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-oss-connector-name}/stop
```

## rocketmq-connect-oss 参数说明
* **oss-sink-connector 参数说明**

|         KEY            |  TYPE   | Must be filled | Description                      | Example
|------------------------|---------|----------------|----------------------------------|---------|
|accountEndpoint         | String  | YES            | OSS endpoint                     | oss-cn-beijing.aliyuncs.com|
|accessKeyId             | String  | YES            | 阿里云授信账号的AK                 | xxxx |
|accessKeySecret         | String  | YES            | 阿里云授信账号的SK                 | xxx  |
|bucketName              | String  | YES            | OSS bucketName                   | test_bucket |
|objectName              | String  | YES            | 上传目的object名字                | test.txt |
|region                  | String  | YES            | OSS region                       | cn-beijing |
|partitionMethod         | String  | YES            | 分区模式，Normal表示不分区，Time表示按时间分区 | Time |
|fileUrlPrefix           | String  | YES            | 到object的URL前缀                 | file1/ |

