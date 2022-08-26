**rocketmq-connect-kafka-connector-adapter**

本项目的目标是让kafka connector运行在rocketmq-connect，使得数据在rocketmq导入导出。

**参数说明**

参数分为3类：rocketmq connect runtime参数、 kafka-connector-adapter参数，以及 具体kafka connector参数

rocketmq connect runtime参数：
- **connector-class**: kafka-connector-adapter的类名
  
  如果是SourceConnector，对应为org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSourceConnector。
  
  如果是SinkConnector，对应为org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSinkConnector。
  
- **connect-topicname**: 要导入导出数据的rocketmq topic
- **tasks.num**: 启动的task数目 
  
kafka-connector-adapter参数：
- **connector.class**: kafka connector的类名
- **plugin.path**: kafka connector插件路径

具体kafka connector参数：

参考具体kafka connector的文档


# 快速开始

demo展示如何启动kafka-file-connector

适配的kafka-file-connector的主要作用是从源文件中读取数据发送到RocketMQ集群 然后从Topic中读取消息，写入到目标文件

## 1.获取kafka-file-connector

1. 下载kafka的二进制包:https://kafka.apache.org/downloads
2. 解压后到libs目录找到kafka-file-connector的jar包：connect-file-{version}.jar
3. 将jar拷贝到专门目录,这个目录作为kafka connector插件路径：plugin.path，比如：/tmp/kafka-plugins


## 2.构建rocketmq-connect-kafka-connector-adapter

```
git clone https://github.com/apache/rocketmq-connect.git

cd  connectors/rocketmq-connect-kafka-connector-adapter/

mvn package

```
最后将/target/rocketmq-connect-kafka-connector-adapter-0.0.1-SNAPSHOT-jar-with-dependencies.jar拷贝到rocketmq插件目录下，并修改connect-standalone.conf的pluginPaths为对应的rocketmq插件目录
，比如/tmp/rocketmq-plugins

## 3.运行Worker

```
cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT

sh bin/connect-standalone.sh -c conf/connect-standalone.conf &

```

## 4.启动source connector

```
touch /tmp/test-source-file.txt

echo "Hello \r\nRocketMQ\r\n Connect" >> /tmp/test-source-file.txt

curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSourceConnector -d '{"connector-class":"org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSourceConnector","connect-topicname":"fileTopic","connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector","plugin.path":"/tmp/kafka-plugins","topic":"fileTopic","file":"/tmp/test-source-file.txt"}'
```

## 5.启动sink connector

```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSinkConnector -d '{"connector-class":"org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSinkConnector","connect-topicname":"fileTopic","connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector","plugin.path":"/tmp/kafka-plugins","file":"/tmp/test-sink-file.txt"}'

cat /tmp/test-sink-file.txt
```

# kafka connect transform

todo

# 如何运行kafka-mongo-connector

todo

# 如何运行kafka-jdbc-connector

todo