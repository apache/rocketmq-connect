这篇文章将以一个connector贡献者的角度展开描述，主要介绍RocketMQ-Connect的整体设计思路、与connector实现相关的关键类以及如何实现一个connector。

## RocketMQ-Connect的整体感知

RocketMQ-Connect是连接各类存储介质（包括各种数据库、消息中间件以及文件存储等）到RocketMQ的桥梁，在一定程度上扩展了RocketMQ的功能和使用范围。

一个典型的应用场景是，通过使用RocketMQ-Connect，能够实现异构数据库之间的数据同步工作：举例来说，我们可能希望将Mysql数据库中的数据同步到ElasticSearch中来达成某些分析或查找的目的。以往的做法是，需要编写一个服务定时地从MySql中抽取数据，并对数据进行格式转换后写入到ElasticSearch。这种做法往往有着很大的性能局限和维护成本。而通过使用RocketMQ-Connect，仅仅需要配置源端连接器SourceConnector和目的端连接器SinkConnector即可将MySql和ElasticSearch数据库进行连接，并且实现服务的解耦和削峰，避免因源数据库的数据激增造成服务崩溃的情况。

直观来讲，上述例子构建了一条MySql->RocketMQ->ElasticSearch的数据链路。RocketMQ-Connect的Source端以及Sink端实际上是两个RocketMQ客户端，Source端相当于Producer，Sink端相当于Cosumer。Source的上游是源端数据库，下游是RocketMQ，它通过某些手段监听上游数据库表中的数据变化（可能是定时轮询，如JDBC方式，也可能是CDC监听等等），并通过Producer客户端将数据发送给RocketMQ。Sink的上游是RocketMQ，下游是目的端数据库，通过消费RocketMQ中的数据，将数据写入到目的端的数据库中。Source和Sink通过同一套规则绑定到同一个topic上，通过RocketMQ实现了服务的解耦，能够在稳定服务的同时实现两端connector的灵活搭配。

## RocketMQ-Connect中的关键概念

想要实现一个connector，需要对RocketMQ-Connect中与connector实现相关的类和核心概念有所熟悉。

### connector

connector是RocketMQ和不同数据源之间的连接器，分为源端的SourceConnector和目的端的SinkConnector。connector实际上是暴露给用户进行管理的，用户可以通过RestFul API来创建、删除connector，实际上就是创建了一条条的数据链路，并通过发送开始/暂停指令来管理数据链路的启停。

### Task

Task是链路中进行操作数据的主体，内部定义了操作数据的流程，例如如何从数据库中获取数据以及如何将数据写入到数据库中等等。Task的开始和暂停由connector进行管理，也就是说，当用户向某一个connector发送开始/暂停指令时，实际上是开启或暂停了对应的Task。值得注意的是，对于同一类型的connector来说，内部的运行机制实际上是一致的，也就是其共享task里的执行逻辑。

### WorkerTask

WorkerTask是RocketMQ-Connect里真正的“苦力”，每个WorkerTask都是一个Runnable线程，根据不同的使用场景，又分为了WorkerSourceTask以及WorkerSinkTask，分别对应Source端场景和Sink端场景。

WorkerSinkTask内部使用了DefaultLitePullConsumer来从RocketMQ中获取数据，同时保存了一个SinkTask对象，在收到消息以后调用SinkTask的`put()`方法来处理数据（也就是写入目的端数据库）；类似地，WorkerSourceTask内部使用了DefaultMQProducer作为客户端，也保存了一个SourceTask对象，它先调用SourceTask的`poll()`方法从源端获取数据，再将获得到的数据发送到RocketMQ中。

### Worker

Worker类主要负责各个WorkerTask的调度工作，内部通过线程池的方式运行每一个WorkTask。

## 如何实现一个connector

实现一个connector的关键在于实现对应的SourceConnector/SinkConnector以及SourceTask/SinkTask。

### Source端

其中Source端需要继承SourceConnector和SourceTask抽象类，下面依次描述一下其中的方法：

```java
public abstract class SourceConnector extends Connector {
    public List<KeyValue> taskConfigs(int maxTasks); // 返回任务的配置信息
    public Class<? extends Task> taskClass(); // 返回对应task的类
    public void start(KeyValue config); // 开启connector的行为
    public void stop(); // 关闭connector的行为
}
```

```java
public abstract class SourceTask implements Task<SourceTaskContext> {
    public abstract List<ConnectRecord> poll() throws InterruptedException; // 实现拉取数据的逻辑，需包装成ConnectorRecord    
    void start(KeyValue var1); // 开启task的行为
    void stop(); // 关闭task的行为    
}
```

### Sink端

sink端同样需要继承SinkConnector和SinkTask抽象类，其中SinkConnector和SourceConnector都继承自Connector抽象类，因此内部方法和原理基本一致。下面介绍一下SinkTask抽象类。

```java
public abstract class SinkTask implements Task<SinkTaskContext> {
    public abstract void put(List<ConnectRecord> var1) throws ConnectException; // 实现数据写入逻辑，将RocketMQ中的数据写入目的端数据库
    void start(KeyValue var1); // 开启task的行为
    void stop(); // 关闭task的行为  
}
```

## 如何验证自己的connector

1. 在工程中新建一个module并实现connector

2. 将connector对应的module使用`mvn package`打包成jar文件

3. 修改`distribution/conf/connect-standalone.conf`中的pluginPaths属性为自己jar包的位置

4. 在IDE中启动RocketMQ-Connect，过程参考[快速开始](https://github.com/apache/rocketmq-connect#readme)

5. 使用API工具（如Postman）向本地对应端口发送创建connector指令，指令内容可以参考对应connector的README文件。

6. 查看效果是否符合预期或进行调试
