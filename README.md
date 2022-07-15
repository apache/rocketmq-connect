# rocketmq-connect
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

文档中心：[RocketMQ Connect](https://rocketmq-1.gitbook.io/rocketmq-connector/)

架构简介：[RocketMQ Connect 平台搭建与实践](https://developer.aliyun.com/search?q=rocketmq+connect&bizCategory=ucc-file) 

# 快速开始

单机模式下[rocketmq-connect-sample](https://rocketmq-1.gitbook.io/rocketmq-connector/quick-start/file-connector)作为 demo

rocketmq-connect-sample的主要作用是从源文件中读取数据发送到RocketMQ集群 然后从Topic中读取消息，写入到目标文件 

## 1.准备

1. Linux/Unix/Mac
2. 64bit JDK 1.8+;
3. Maven 3.2.x或以上版本;
4. 启动 [RocketMQ](https://rocketmq.apache.org/docs/quick-start/);
5. 创建测试Topic
    sh  ${ROCKETMQ_HOME}/bin/mqadmin updateTopic -t fileTopic -n localhost:9876 -c DefaultCluster -r 8 -w 8

**tips** : ${ROCKETMQ_HOME} 位置说明

bin-release.zip 版本：/rocketmq-all-4.9.4-bin-release

source-release.zip 版本：/rocketmq-all-4.9.4-source-release/distribution


## 2.构建Connect

```
git clone https://github.com/apache/rocketmq-connect.git

cd  rocketmq-connect

mvn -Prelease-connect -DskipTests clean install -U

```

## 3.运行Worker

```
cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT

sh bin/connect-standalone.sh -c conf/connect-standalone.conf &

```
**tips**: 可修改 /bin/runconnect.sh 适当调整 JVM Parameters Configuration

```
JAVA_OPT="${JAVA_OPT} -server -Xms256m -Xmx256m"
```
runtime启动成功：

The standalone worker boot success.

查看启动日志文件：

tail -100f ~/logs/rocketmqconnect/connect_runtime.log

ctrl + c 退出日志

## 4.启动source connector

当前目录创建测试文件 test-source-file.txt
```
touch test-source-file.txt

echo "Hello \r\nRocketMQ\r\n Connect" >> test-source-file.txt

curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSourceConnector -d '{"connector-class":"org.apache.rocketmq.connect.file.FileSourceConnector","filename":"test-source-file.txt","connect-topicname":"fileTopic"}'
```

看到以下日志说明 file source connector 启动成功了

tail -100f ~/logs/rocketmqconnect/connect_runtime.log

2019-07-16 11:18:39 INFO pool-7-thread-1 - **Source task start**, config:{"properties":{"source-record-...

#### source connector配置说明

| key                     | nullable | default               | description |
| ----------------------- | -------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| connector-class         | false    |                                                  |实现 Connector接口的类名称（包含包名）|
| filename                | false    |                                                                          | 数据源文件名称 |
| connect-topicname       | false    |                                                                   |同步文件数据所需topic|


## 5.启动sink connector

```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSinkConnector -d '{"connector-class":"org.apache.rocketmq.connect.file.FileSinkConnector","filename":"test-sink-file.txt","connect-topicname":"fileTopic"}'

cat test-sink-file.txt
```

tail -100f ~/logs/rocketmqconnect/connect_runtime.log

看到以下日志说明file sink connector 启动成功了

2019-07-16 11:24:58 INFO pool-7-thread-2 - **Sink task start**, config:{"properties":{"source-record-...

如果 test-sink-file.txt 生成并且与 source-file.txt 内容一样，说明整个流程正常运行。
文件内容可能顺序不一样，这主要是因为RocketMQ发到不同queue时，接收不同queue消息顺序可能也不一致导致的，是正常的。

#### sink connector配置说明

| key                     | nullable | default | description                                                                            |
| ----------------------- | -------- | ------- | -------------------------------------------------------------------------------------- |
| connector-class         | false    |         | 实现Connector接口的类名称（包含包名）                                                  |
| filename      | false    |         | sink拉去的数据保存到文件                                               |
| connect-topicname | false    |         | sink需要处理数据消息topics                                             |

```  
注：source/sink配置文件说明是以rocketmq-connect-sample为demo，不同source/sink connector配置有差异，请以具体sourc/sink connector 为准
```

## 6.停止connector

```shell
GET请求  
http://(your worker ip):(port)/connectors/(connector name)/stop

停止demo中的两个connector
curl     http://127.0.0.1:8082/connectors/fileSinkConnector/stop
curl     http://127.0.0.1:8082/connectors/fileSourceConnector/stop
    
```
看到以下日志说明connector停止成功了

**Source task stop**, config:{"properties":{"source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter","filename":"/home/zhoubo/IdeaProjects/my-new3-rocketmq-externals/rocketmq-connect/rocketmq-connect-runtime/source-file.txt","task-class":"org.apache.rocketmq.connect.file.FileSourceTask","topic":"fileTopic","connector-class":"org.apache.rocketmq.connect.file.FileSourceConnector","update-timestamp":"1564765189322"}}

## 7.停止Worker进程

```
sh bin/connectshutdown.sh
```


## 8.日志目录

 ${user.home}/logs/rocketmqconnect 

## 9.配置文件

持久化配置文件默认目录 /tmp/storeRoot

1. connectorConfig.json connector配置持久化文件
2. position.json        source connect数据处理进度持久化文件
3. taskConfig.json      task配置持久化文件
4. offset.json          sink connect数据消费进度持久化文件

## 10.配置说明

可根据使用情况修改 [RESTful](https://restfulapi.cn/) 端口，storeRoot 路径，Nameserver 地址等信息

文件位置：work 启动目录下 conf/connect-standalone.conf

```shell
#current cluster node uniquely identifies
workerId=DEFAULT_WORKER_1

# Http prot for user to access REST API
httpPort=8082

# Local file dir for config store
storePathRootDir=/home/connect/storeRoot

#需要修改为自己的rocketmq nameserver 接入点
# RocketMQ namesrvAddr
namesrvAddr=127.0.0.1:9876  

#用于加载Connector插件，类似于jvm启动加载jar包或者class类，这里目录目录用于放Connector相关的实现插件，
支持文件和目录
# Source or sink connector jar file dir
pluginPaths=rocketmq-connect-sample/target/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar

# 补充：将 Connector 相关实现插件保存到指定文件夹 
# pluginPaths=/usr/local/connector-plugins/*
```

## 11.其它restful接口


查看集群节点信息：

```
curl -X GET http://(your worker ip):(port)/getClusterInfo
```

查看集群中Connector和Task配置信息：

```
curl -X GET http://(your worker ip):(port)/getConfigInfo
```

查看当前节点分配Connector和Task配置信息：

```
curl -X GET http://(your worker ip):(port)/getAllocatedInfo
```

查看指定Connector配置信息：

```
curl -X GET http://(your worker ip):(port)/connectors/(connector name)/config
```

查看指定Connector状态：

```
curl -X GET http://(your worker ip):(port)/connectors/(connector name)/status
```

停止所有Connector：

```
curl -X GET http://(your worker ip):(port)/connectors/stopAll
```

重新加载Connector插件目录下的Connector包：

```
curl -X GET http://(your worker ip):(port)/plugin/reload
```

从内存删除Connector配置信息（谨慎使用）：

```
curl -X GET http://(your worker ip):(port)/connectors/(connector name)/delete
```


## 12.runtime配置参数说明

| key                      | nullable | default                                                      | description                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| workerId                 | false    | DEFAULT_WORKER_1                                             | 集群节点唯一标识                                             |
| namesrvAddr              | false    | loacalhost:9876                                              | RocketMQ Name Server地址列表，多个NameServer地址用分号隔开   |
| httpPort                 | false    | 8082                                                         | runtime提供restful接口服务端口                               |
| pluginPaths              | false    |                                                              | source或者sink目录，启动runttime时加载                       |
| storePathRootDir         | true     | /tmp/connectorStore                                          | 持久化文件保存目录                                           |
| positionPersistInterval  | true     | 20s                                                          | source端持久化position数据间隔                               |
| offsetPersistInterval    | true     | 20s                                                          | sink端持久化offset数据间隔                                   |
| configPersistInterval    | true     | 20s                                                          | 集群中配置信息持久化间隔                                     |
| rmqProducerGroup         | true     | defaultProducerGroup                                         | Producer组名，多个Producer如果属于一个应用，发送同样的消息，则应该将它们归为同一组 |
| rmqConsumerGroup         | true     | defaultConsumerGroup                                         | Consumer组名，多个Consumer如果属于一个应用，发送同样的消息，则应该将它们归为同一组 |
| maxMessageSize           | true     | 4MB                                                          | RocketMQ最大消息大小                                         |
| operationTimeout         | true     | 3s                                                           | Producer发送消息超时时间                                     |
| rmqMaxRedeliveryTimes    | true     |                                                              | 最大重新消费次数                                             |
| rmqMessageConsumeTimeout | true     | 3s                                                           | Consumer超时时间                                             |
| rmqMaxConsumeThreadNums  | true     | 32                                                           | Consumer客户端最大线程数                                     |
| rmqMinConsumeThreadNums  | true     | 1                                                            | Consumer客户端最小线程数                                     |
| allocTaskStrategy        | true     | org.apache.rocketmq.connect.<br>runtime.service.strategy.<br>DefaultAllocateConnAndTaskStrategy | 负载均衡策略类                                               |

### allocTaskStrategy说明

该参数默认可省略，这是一个可选参数，目前选项如下：

* 默认值
```java
org.apache.rocketmq.connect.runtime.service.strategy.DefaultAllocateConnAndTaskStrategy
```

* 一致性Hash
```java
org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategyByConsistentHash
```
### 更多集群和负载均衡文档

[负载均衡](https://rocketmq-1.gitbook.io/rocketmq-connector/rocketmq-connect-1/rocketmq-runtime/fu-zai-jun-heng)

## 13.runtime支持JVM参数说明

| key                                             | nullable | default | description             |
| ----------------------------------------------- | -------- | ------- | ----------------------- |
| rocketmq.runtime.cluster.rebalance.waitInterval | true     | 20s     | 负载均衡间隔            |
| rocketmq.runtime.max.message.size               | true     | 4M      | Runtime限制最大消息大小 |
|[virtualNode](#virtualnode)       |true    |  1        | 一致性hash负载均衡的虚拟节点数|
|[consistentHashFunc](#consistenthashfunc)|true    |MD5Hash|一致性hash负载均衡算法实现类|

### VirtualNode 

一致性hash中虚拟节点数

### consistentHashFunc

hash算法具体实现类,可以自己实现，在后续版本中会增加更多策略，该类应该实现

```java

org.apache.rocketmq.common.consistenthash.HashFunction;

package org.apache.rocketmq.common.consistenthash;

public interface HashFunction {
    long hash(String var1);
}

```

默认情况下采用的是`MD5Hash`算法

```java
//default hash function
private static class MD5Hash implements HashFunction {
    MessageDigest instance;

    public MD5Hash() {
        try {
            instance = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
        }
    }

    @Override
    public long hash(String key) {
        instance.reset();
        instance.update(key.getBytes());
        byte[] digest = instance.digest();

        long h = 0;
        for (int i = 0; i < 4; i++) {
            h <<= 8;
            h |= ((int) digest[i]) & 0xFF;
        }
        return h;
    }
}
```

##开发指南
如何在IDE中启动Connect Worker ?

###单机模式启动Connect Worker

Main Class配置
org.apache.rocketmq.connect.runtime.StandaloneConnectStartup

![img_2.png](img_2.png)

Program arguments配置

-c  ${user path}/rocketmq-connect/distribution/conf/connect-standalone.conf

Environment variables配置

CONNECT_HOME=${user path}/rocketmq-connect/distribution

###集群模式启动Connect Worker

Main Class配置
org.apache.rocketmq.connect.runtime.StandaloneConnectStartup


Program arguments配置

-c  ${user path}/rocketmq-connect/distribution/conf/connect-standalone.conf

Environment variables配置

CONNECT_HOME=${user path}/rocketmq-connect/distribution





