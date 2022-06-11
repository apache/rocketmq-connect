# rocketmq-connect
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# GitBook文档

[快速开始](https://rocketmq-1.gitbook.io/rocketmq-connector/quick-start/qian-qi-zhun-bei)

[Runtime](https://rocketmq-1.gitbook.io/rocketmq-connector/rocketmq-connect-1/rocketmq-runtime)

# 快速开始

文档以rocketmq-connect-sample作为demo

## 1.准备

1. 64bit JDK 1.8+;

2. Maven 3.2.x或以上版本;

3. Start the [RocketMQ](https://rocketmq.apache.org/docs/quick-start/) Service;

4. 创建 connectors 存放目录:
```
mkdir ~/connectors
```

5. 创建 connector runtime 配置存放目录:
```
mkdir ~/storeRoot
```

## 2.构建

```
mvn clean install -Dmaven.test.skip=true
mv rocketmq-connect-sample/target/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar ~/connectors
```

## 3.配置


cd rocketmq-connect-runtime/target/distribution/conf
1. 修改配置文件 vi connect-distributed.conf

```
#current cluster node uniquely identifies
workerId=DEFAULT_WORKER_1

# Http prot for user to access REST API
httpPort=8081

# Local file dir for config store
storePathRootDir=~/storeRoot

#需要修改为自己的rocketmq nameserver 接入点
# RocketMQ namesrvAddr
namesrvAddr=127.0.0.1:9876  

#修改为 1.5 小节中创建的connectors 加载目录，加载 sample 中 source/sink connector
# Source or sink connector jar file dir
pluginPaths=~/connectors
``` 

## 4.运行

返回 rocketmq-connect-runtime 目录执行：

```
sh ./connect-distributed.sh
```

查看日志文件${user.home}/logs/rocketmqconnect/connect_runtime.log
以下日志表示runtime启动成功：

The worker [DEFAULT_WORKER_1] boot success.

```
注：启动之前 RocketMQ 创建以下topic
connector-cluster-topic 集群信息
connector-config-topic  配置信息
connector-offset-topic  sink消费进度
connector-position-topic source数据处理进度
并且为了保证消息有序，每个 topic 可以只建一个 queue
```

## 5.日志目录

 ${user.home}/logs/rocketmqconnect 

## 6.配置文件

持久化配置文件默认目录 ～/storeRoot

1. connectorConfig.json connector配置持久化文件
2. position.json        source connect数据处理进度持久化文件
3. taskConfig.json      task配置持久化文件
4. offset.json          sink connect数据消费进度持久化文件

## 7.启动source connector

提前创建测试文件~/test/test-source-file.txt，随意写一些内容在里面

```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSourceConnector -d '{"connector-class":"org.apache.rocketmq.connect.file.FileSourceConnector","filename":"~/test/test-source-file.txt","connect-topicname":"fileTopic"}'
```

   看到一下日志说明file source connector启动成功了
   
   2019-07-16 11:18:39 INFO pool-7-thread-1 - Source task start, config:{"properties":{"source-record-...
```  
    注：创建topic："connect-topicname":"fileTopic"
```

#### source connector配置说明

| key                     | nullable | default | description                                                                            |
| ----------------------- | -------- | ------- | -------------------------------------------------------------------------------------- |
| connector-class         | false    |         | 实现Connector接口的类名称（包含包名）                                                  |
| filename                | false    |         | 数据源文件名称                                                                         |
| task-class              | false    |         | 实现SourceTask类名称（包含包名）                                                       |
| connect-topicname                   | false    |         | 同步文件数据所需topic                                                                  |
| update-timestamp        | false    |         | 配置更新时间戳                                                                         |
| source-record-converter | false    |         | Full class name of the impl of the converter used to convert SourceDataEntry to byte[] |


## 8.启动sink connector

```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/fileSinkConnector -d '{"connector-class":"org.apache.rocketmq.connect.file.FileSinkConnector","filename":"~/test/test-sink-file.txt","connect-topicname":"fileTopic"}'
```  
看到一下日志说明file sink connector启动成功了

2019-07-16 11:24:58 INFO pool-7-thread-2 - Sink task start, config:{"properties":{"source-record-...

查看配置中"filename":"/home/connect/rocketmq-externals/rocketmq-connect-runtime/sink-file.txt"配置文件
如果sink-file.txt生成并且与source-file.txt内容一样，说明整个流程已经跑通

#### sink connector配置说明

| key                     | nullable | default | description                                                                            |
| ----------------------- | -------- | ------- | -------------------------------------------------------------------------------------- |
| connector-class         | false    |         | 实现Connector接口的类名称（包含包名）                                                  |
| connect-topicname              | false    |         | sink需要处理数据消息topics                                                             |
| task-class              | false    |         | 实现SourceTask类名称（包含包名）                                                       |
| filename                | false    |         | sink拉去的数据保存到文件                                                               |
| update-timestamp        | false    |         | 配置更新时间戳                                                                         |
| source-record-converter | false    |         | Full class name of the impl of the converter used to convert SourceDataEntry to byte[] |

```  
注：source/sink配置文件说明是以rocketmq-connect-sample为demo，不同source/sink connector配置有差异，请以具体sourc/sink connector为准
```  

## 9.停止connector

```
    GET请求  
    http://(your worker ip):(port)/connectors/(connector name)/stop
```  
看到一下日志说明connector停止成功了

Source task stop, config:{"properties":{"source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter","filename":"/home/zhoubo/IdeaProjects/my-new3-rocketmq-externals/rocketmq-connect/rocketmq-connect-runtime/source-file.txt","task-class":"org.apache.rocketmq.connect.file.FileSourceTask","topic":"fileTopic","connector-class":"org.apache.rocketmq.connect.file.FileSourceConnector","update-timestamp":"1564765189322"}}


## 10.其它restful接口


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


## 11.runtime配置参数说明

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

## 12.runtime支持JVM参数说明

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





