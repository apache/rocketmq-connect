假设rocketmq有2个节点，一个brokerName是broker-0，另一个brokerName是broker-1


## 1.启动测试 neo4j server
```
docker run \
    --publish=7474:7474 --publish=7687:7687 \
    --volume=$HOME/neo4j/data:/data \
	--env=NEO4J_AUTH=neo4j/connect \
    neo4j
```

账号是neo4j，密码是connect

## 2.运行neo4j sink connector
```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/neo4jSinkConnector -d '{
    "connector.class": "org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSinkConnector",
    "connect.topicnames":"my-topic",
	"kafka.connector.configs":{
		"rocketmq.recordPartition.kafkaTopicPartition.mapper": "assignEncodedPartition",
		"assignEncodedPartition.assignments": "broker-0:0,broker-1:1",
		"connector.class": "streams.kafka.connect.sink.Neo4jSinkConnector",
		"key.converter": "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable": false,
		"value.converter": "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": false,
		"errors.retry.timeout": "-1",
		"errors.retry.delay.max.ms": "1000",
		"errors.tolerance": "all",
		"errors.log.enable": true,
		"errors.log.include.messages": true,
		"neo4j.server.uri": "bolt://localhost:7687",
		"neo4j.authentication.basic.username": "neo4j",
		"neo4j.authentication.basic.password": "connect",
		"neo4j.encryption.enabled": false,
		"neo4j.topic.cypher.my-topic": "MERGE (p:Person{name: event.name, surname: event.surname}) MERGE (f:Family{name: event.surname}) MERGE (p)-[:BELONGS_TO]->(f)",
		"plugin.path": "/kafka-connecor-plugins/neo4j-connector"
	}
}'
```
其中assignEncodedPartition.assignments指定brokerName的编码，例子是broker-0:0,broker-1:1

## 3.测试
1、给topic发送消息：
sh mqadmin sendMessage -n localhost:9876 -t my-topic -p '{"name": "Name", "surname": "Surname"}'

2、到neo4j查询
浏览器登录：http://localhost:7474/browser/

query：

MATCH  (ee:Person) WHERE ee.name ='Name' RETURN  ee;


更多关于connector：

https://neo4j.com/labs/kafka/4.0/kafka-connect/