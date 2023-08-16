# rocketmq-replicator

## rocketmq-replicator 简介

![image](https://blobscdn.gitbook.com/v0/b/gitbook-28427.appspot.com/o/assets%2F-Lm4-doAUYYZgDcb_Jnz%2F-LoOhyGfSf-N6oHVgJhr%2F-LoOi0ADfZ4q-qPo_uEB%2Frocketmq%20connector.png?alt=media&token=0bbbfa54-240a-489e-8dfb-1996d0800dfc)

Replicator 用于 RocketMQ 集群之间的消息同步，实现跨集群消息路由，可以用在 RocketMQ 异地多活，容灾等场景。Replicator 是在 RocketMQ Connect 框架基础上实现的，作为一个 connector 运行在 RocketMQ Connect 的 Runtime 中。

## RocketMQ Connect 文档

[RocketMQ Connect 文档](https://rocketmq.apache.org/zh/docs/connect/01RocketMQ%20Connect%20Overview/)

# Replicator 快速开始

---


## rocketmq-replicator打包

````
cd ./rocketmq-connect/connectors/rocketmq-replicator

mvn clean install -Prelease-all -DskipTest -U 
````

打包成功后将` rocketmq-replicator-0.1.0-SNAPSHOT-jar-with-dependencies.jar `（./rocketmq-connect/connectors/rocketmq-replicator/target目录下）放到 runtime 配置的 pluginPaths 目录下
详细查看 [RocketMQ Connect 快速开始](https://rocketmq.apache.org/zh/docs/connect/03RocketMQ%20Connect%20Quick%20Start) 配置说明

## rocketmq-replicator启动

同步消息
````
curl -X POST -H "Content-Type: application/json" http://${runtime-port}:${runtime-ip}/connectors/${replicator-name} -d '{
    "connector.class": "org.apache.rocketmq.replicator.ReplicatorSourceConnector",
    "src.cluster": "${srcDefaultCluster}",
    "src.endpoint": "${namesrvEndpoint}",
    "dest.acl.enable": "false",
    "src.secret.key": "${sk}",
    "dest.topic": "${targetClusterTopic}",
    "dest.access.key": "${ak}",
    "max.tasks": "2",
    "src.topictags": "test1,*",
    "src.acl.enable": "false",
    "errors.tolerance": "all",
    "dest.secret.key": "${sk}",
    "dest.endpoint": "${namesrvEndpoint}",
    "src.access.key": "${ak}",
    "dest.cluster": "${targetDefaultCluster}",
    "source.cluster": "${sourceDefaultCluster}",
    "dest.region": "${regionA}",
    "src.region": "${regionB}",
    "dest.cloud": "${cloud1}",
    "source.cloud": "${cloud2}"
}'
````
例如
````
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/test_replicator4 -d '{
    "connector.class": "org.apache.rocketmq.replicator.ReplicatorSourceConnector",
    "src.endpoint": "127.0.0.2:9876",
    "src.cluster": "DefaultCluster",
    "src.region": "regionA",
    "src.cloud": "src-cloud",
    "dest.acl.enable": "false",
    "dest.topic": "TopicTest",
    "max.task": "2",
    "src.topictags": "TopicTest,*",
    "src.acl.enable": "false",
    "dest.endpoint": "127.0.0.1:9876",
    "dest.region": "regionB",
    "dest.cluster": "DefaultCluster",
    "errors.tolerance": "all",
    "dest.cloud": "dest-cloud"
}'
````

同步位点

````
curl -X POST -H "Content-Type: application/json" http://${runtime-port}:${runtime-ip}/connectors/${replicator-name} -d '{
    "connector.class": "org.apache.rocketmq.replicator.ReplicatorCheckpointConnector",
    "src.cluster": "${srcDefaultCluster}",
    "src.endpoint": "${namesrvEndpoint}",
    "dest.acl.enable": "false",
    "src.secret.key": "${sk}",
    "dest.access.key": "${ak}",
    "src.topictags": "test1",
    "sync.gids":"test_gid1",
    "src.acl.enable": "false",
    "errors.tolerance": "all",
    "dest.secret.key": "${sk}",
    "dest.endpoint": "${namesrvEndpoint}",
    "src.access.key": "${ak}",
    "dest.cluster": "${targetDefaultCluster}",
    "source.cluster": "${sourceDefaultCluster}",
    "dest.region": "${regionA}",
    "src.region": "${regionB}",
    "dest.cloud": "${cloud1}",
    "source.cloud": "${cloud2}",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}'
````
例如
````
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/test_checkpoint_replicator -d '{
    "connector.class": "org.apache.rocketmq.replicator.ReplicatorCheckpointConnector",
    "src.endpoint": "127.0.0.1:9876",
    "src.cluster": "DefaultCluster",
    "src.region": "regionA",
    "src.cloud": "cloud1",
    "dest.acl.enable": "false",
    "src.topictags": "TopicTest",
    "src.acl.enable": "false",
    "dest.endpoint": "127.0.0.2:9876",
    "dest.region": "regionB",
    "dest.cluster": "DefaultCluster",
    "errors.tolerance": "all",
    "sync.gids":"test_gid1",
    "dest.cloud": "dest-cloud",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}'
````
同步位点任务创建成功以后，可以再目的 RocketMQ 集群 replicator_checkpoint Topic中消费到位点信息

位点信息格式

````
Struct{consumerGroup=test_gid1,topic=TopicTest,upstreamLastTimestamp=1681962375040,downstreamLastTimestamp=1681439588399,metadata=1682068439879}
````

parameter | type| description                            |
---|-------------|----------------------------------------|
consumerGroup | String      | consumerGroup                          |                                                                                                                                                                                                                                                                           
topic | String      | topic                                  |    
upstreamLastTimestamp | long        | 源 RocketMQ  topic consumerGourp 的消费位点  |  
downstreamLastTimestamp | long        | 目的 RocketMQ  topic consumerGourp 的消费位点 |  

## rocketmq-replicator停止
````
curl http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-replicator-name}/stop
````

## rocketmq-replicator参数说明

parameter | type | must | description                                                                                                                                                                                                                                                                                                                              | sample value   
---|---|------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
src.endpoint | String | Yes  | namesrv address of source rocketmq cluster                                                                                                                                                                                                                                                                                               | 127.0.0.1:9876 |
src.topictags | String | Yes  | source cluster topic and tag，${topic},{tag}                                                                                                                                                                                                                                                                                              | test1,*        |
dest.topic | String | Yes  | target cluster topic                                                                                                                                                                                                                                                                                                                     | test2          |
dest.endpoint | String | Yes   | namesrv address of target rocketmq cluster                                                                                                                                                                                                                                                                                               | 127.0.0.1:9876 |
max.tasks | String | No   | maximum number of tasks                                                                                                                                                                                                                                                                                                                  | 2              |
dest.acl.enable | String | No  | acl switch,enumeration value : true/false                                                                                                                                                                                                                                                                                                | false          |
dest.access.key | String | No  | please refer to the RocketMQ ACL module，when dest.acl.enable is false, this parameter does not take effect                                                                                                                                                                                                                               | accesskey      |
dest.secret.key | String | No   | please refer to the RocketMQ ACL module，when dest.acl.enable is false, this parameter does not take effect                                                                                                                                                                                                                               | secretkey      |
src.acl.enable | String | No  | acl switch,enumeration value : true/false                                                                                                                                                                                                                                                                                                | true           |
src.access.key | String | No  | please refer to the RocketMQ ACL module，when dest.acl.enable is false, this parameter does not take effect                                                                                                                                                                                                                               | accesskey      |
src.secret.key | String | No   | please refer to the RocketMQ ACL module，when dest.acl.enable is false, this parameter does not take effect                                                                                                                                                                                                                               | secretkey      |
errors.tolerance | String | No   | error tolerance  ，enumeration value : all . all means to tolerate all errors, the synchronization message failure will be skipped and error log will be printed. If there is no error tolerance configured, all errors will not be tolerated by default, a synchronization failure occurs, and the task will stop after multiple retries | all            |
src.cluster | String | No   | source cluster                                                                                                                                                                                                                                                                                                                           | DefaultCluster |
dest.cluster | String | No   | target cluster                                                                                                                                                                                                                                                                                                                           | DefaultCluster |
src.region | String | No   | source region                                                                                                                                                                                                                                                                                                                            | regionA        |
dest.region | String | No   | source region                                                                                                                                                                                                                                                                                                                            | regionB        |
src.cloud | String | No   | source cloud                                                                                                                                                                                                                                                                                                                             | cloud1         |
dest.cloud | String | No   | source cloud                                                                                                                                                                                                                                                                                                                             | cloud2         |
sync.gids | String | No   | consumeGroup                                                                                                                                                                                                                                                                                                                             | consumerGroup1 |
key.converter | String | No   | key.converter                                                                                                                                                                                                                                                                                                                           | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter         |
value.converter | String | No   | value converter                                                                                                                                                                                                                                                                                                                          | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter         |
                                                                                                                                                                                                                                                                                      
                                                                                                                                                                                                                                                                                      
