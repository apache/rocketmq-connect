假设rocketmq有2个节点，一个brokerName是broker-0，另一个brokerName是broker-1

## 1.mongo source connector
```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/mongo-source -d '{
	"connector-class": "org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSourceConnector",
	"connect-topicname": "mongoTopic",
	
	"connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
	"plugin.path": "/kafka/connectors",
	
	"connection.uri": "mongodb://127.0.0.1:27017/?replicaSet=rs0",
	"database": "quickstart",
	"collection": "sampleData",
	"pipeline": "[{\"$match\": {\"operationType\": \"insert\"}}, {$addFields : {\"fullDocument.travel\":\"MongoDB Kafka Connector\"}}]"
}'
```

## 2.mongo sink connector
```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/mongo-sink -d '{
"connector-class": "org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSinkConnector",
	"connect-topicname": "mongoTopic",
	"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
	"plugin.path": "/kafka/connectors",
	"connection.uri": "mongodb://mongo1:27017/?replicaSet=rs0",
	"database": "quickstart",
	"collection": "topicData",
	"topics.regex": ".*",
	"change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler"
}'
```