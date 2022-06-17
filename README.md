# rocketmq-connect
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

[Runtime](https://rocketmq-1.gitbook.io/rocketmq-connector/quick-start/runtime-qs)

# 快速开始

以rocketmq-connect-sample作为demo

rocketmq-connect-sample的主要作用是从文件中读取数据发送到RocketMQ集群
然后从Topic中读取消息，写入到目标文件

## 1.准备

1. 64bit JDK 1.8+;

2. Maven 3.2.x或以上版本;

3. 启动RocketMQ， [RocketMQ](https://rocketmq.apache.org/docs/quick-start/);


## 2.构建Connect

```
git clone git@github.com:apache/rocketmq-connect.git

cd  rocketmq-connect

mvn -Prelease-connect -DskipTests clean install -U

```

## 3.运行Worker

```
cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT

sh bin/connect-standalone.sh -c conf/connect-standalone.conf &

```

查看日志文件

tail -100f ~/logs/rocketmqconnect/connect_runtime.log

以下日志表示runtime启动成功：

The standalone worker boot success.

ctrl + c 退出日志

## 4.启动source connector

当前目录创建测试文件test-source-file.txt
```
touch test-source-file.txt

echo "Hello \r\nRocketMQ\r\n Connect" > test-source-file.txt

curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSourceConnector -d '{"connector-class":"org.apache.rocketmq.connect.file.FileSourceConnector","filename":"test-source-file.txt","connect-topicname":"fileTopic"}'
```

看到一下日志说明file source connector启动成功了

2019-07-16 11:18:39 INFO pool-7-thread-1 - Source task start, config:{"properties":{"source-record-...
```  
    注：创建topic：fileTopic
```

#### source connector配置说明

| key                     | nullable | default | description                                                                            |
| ----------------------- | -------- | ------- | -------------------------------------------------------------------------------------- |
| connector-class         | false    |         | 实现Connector接口的类名称（包含包名）                                                  |
| filename                | false    |         | 数据源文件名称                                                                         |
| task-class              | false    |         | 实现SourceTask类名称（包含包名）                                                       |
| connect-topicname                   | false    |         | 同步文件数据所需topic                                                                  |


## 5.启动sink connector

```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSinkConnector -d '{"connector-class":"org.apache.rocketmq.connect.file.FileSinkConnector","filename":"test-sink-file.txt","connect-topicname":"fileTopic"}'

cat test-sink-file.txt
```  
看到一下日志说明file sink connector启动成功了

2019-07-16 11:24:58 INFO pool-7-thread-2 - Sink task start, config:{"properties":{"source-record-...

test-sink-file.txt文件即内容
如果test-sink-file.txt生成并且与source-file.txt内容一样，说明整个流程正常运行

#### sink connector配置说明

| key                     | nullable | default | description                                                                            |
| ----------------------- | -------- | ------- | -------------------------------------------------------------------------------------- |
| connector-class         | false    |         | 实现Connector接口的类名称（包含包名）                                                  |
| connect-topicname              | false    |         | sink需要处理数据消息topics                                                             |
| filename                | false    |         | sink拉去的数据保存到文件                                                               |

```  
注：source/sink配置文件说明是以rocketmq-connect-sample为demo，不同source/sink connector配置有差异，请以具体sourc/sink connector为准
```  

## 6.停止connector

```
    GET请求  
    http://(your worker ip):(port)/connectors/(connector name)/stop
    
    停止demo中的两个connector
    curl     http://127.0.0.1:8082/connectors/fileSinkConnector/stop
    curl     http://127.0.0.1:8082/connectors/fileSourceConnector/stop
    
```  
看到一下日志说明connector停止成功了

Source task stop, config:{"properties":{"source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter","filename":"/home/zhoubo/IdeaProjects/my-new3-rocketmq-externals/rocketmq-connect/rocketmq-connect-runtime/source-file.txt","task-class":"org.apache.rocketmq.connect.file.FileSourceTask","topic":"fileTopic","connector-class":"org.apache.rocketmq.connect.file.FileSourceConnector","update-timestamp":"1564765189322"}}

## 7.停止Worker进程

```
sh bin/connectshutdown.sh

```


## 8.日志目录

 ${user.home}/logs/rocketmqconnect 

## 9.配置文件

持久化配置文件默认目录 ～/storeRoot

1. connectorConfig.json connector配置持久化文件
2. position.json        source connect数据处理进度持久化文件
3. taskConfig.json      task配置持久化文件
4. offset.json          sink connect数据消费进度持久化文件

## 10.配置说明

可根据使用情况修改端口，目录，RocketMQ链接等信息
```
#current cluster node uniquely identifies
workerId=DEFAULT_WORKER_1

# Http prot for user to access REST API
httpPort=8081

# Local file dir for config store
storePathRootDir=/home/connect/storeRoot

#需要修改为自己的rocketmq nameserver 接入点
# RocketMQ namesrvAddr
namesrvAddr=127.0.0.1:9876  

#用于加载Connector插件，类似于jvm启动加载jar包或者class类，这里目录目录用于放Connector相关的实现插件，
支持文件和目录
# Source or sink connector jar file dir
pluginPaths=rocketmq-connect-sample/target/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar
``` 

## 11.其它restful接口


查看集群节点信息：

```
http://(your worker ip):(port)/getClusterInfo
```

查看集群中Connector和Task配置信息：

```
http://(your worker ip):(port)/getConfigInfo
```

查看当前节点分配Connector和Task配置信息：

```
http://(your worker ip):(port)/getAllocatedInfo
```

查看指定Connector配置信息：

```
http://(your worker ip):(port)/connectors/(connector name)/config
```

查看指定Connector状态：

```
http://(your worker ip):(port)/connectors/(connector name)/status
```

停止所有Connector：

```
http://(your worker ip):(port)/connectors/stopAll
```

重新加载Connector插件目录下的Connector包：

```
http://(your worker ip):(port)/plugin/reload
```

从内存删除Connector配置信息（谨慎使用）：

```
http://(your worker ip):(port)/connectors/(connector name)/delete
```


## 12.runtime配置参数说明

| key                      | nullable | default                                                                                         | description                                                                        |
| ------------------------ | -------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| workerId                 | false    | DEFAULT_WORKER_1                                                                                | 集群节点唯一标识                                                                   |
| namesrvAddr              | false    |                                                                                                 | RocketMQ Name Server地址列表，多个NameServer地址用分号隔开                         |
| httpPort                 | false    | 8081                                                                                            | runtime提供restful接口服务端口                                                     |
| pluginPaths              | false    |                                                                                                 | source或者sink目录，启动runttime时加载                                             |
| storePathRootDir         | true     | (user.home)/connectorStore                                                                      | 持久化文件保存目录                                                                 |
| positionPersistInterval  | true     | 20s                                                                                             | source端持久化position数据间隔                                                     |
| offsetPersistInterval    | true     | 20s                                                                                             | sink端持久化offset数据间隔                                                         |
| configPersistInterval    | true     | 20s                                                                                             | 集群中配置信息持久化间隔                                                           |
| rmqProducerGroup         | true     | defaultProducerGroup                                                                            | Producer组名，多个Producer如果属于一个应用，发送同样的消息，则应该将它们归为同一组 |
| rmqConsumerGroup         | true     | defaultConsumerGroup                                                                            | Consumer组名，多个Consumer如果属于一个应用，发送同样的消息，则应该将它们归为同一组 |
| maxMessageSize           | true     | 4MB                                                                                             | RocketMQ最大消息大小                                                               |
| operationTimeout         | true     | 3s                                                                                              | Producer发送消息超时时间                                                           |
| rmqMaxRedeliveryTimes    | true     |                                                                                                 | 最大重新消费次数                                                                   |
| rmqMessageConsumeTimeout | true     | 3s                                                                                              | Consumer超时时间                                                                   |
| rmqMaxConsumeThreadNums  | true     | 32                                                                                              | Consumer客户端最大线程数                                                           |
| rmqMinConsumeThreadNums  | true     | 1                                                                                               | Consumer客户端最小线程数                                                           |
| allocTaskStrategy        | true     | org.apache.rocketmq.connect.<br>runtime.service.strategy.<br>DefaultAllocateConnAndTaskStrategy | 负载均衡策略类                                                                     |

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

private static class MD5Hash implements HashFunction {
        MessageDigest instance;

        public MD5Hash() {
            try {
                this.instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException var2) {
            }

        }

        public long hash(String key) {
            this.instance.reset();
            this.instance.update(key.getBytes());
            byte[] digest = this.instance.digest();
            long h = 0L;

            for(int i = 0; i < 4; ++i) {
                h <<= 8;
                h |= (long)(digest[i] & 255);
            }

            return h;
        }
    }
```

## FAQ

Q1：sink-file.txt文件中每行的文本顺序source-file.txt不一致？

A1: source数据到sink中经过rocketmq中转，如果需要顺序消息，需要有序消息发送到同一个queue。
实现有序消息有2中方式：1、一个topic创建一个queue（rocketmq-connect-runtime目前只能使用这种方式）2、rocketmq-connect-runtime支持顺序消息，通过消息中指定字段处理发送到rocketmq的同一queue（后续支持）





