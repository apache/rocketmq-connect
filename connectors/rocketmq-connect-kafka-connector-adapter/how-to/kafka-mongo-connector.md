假设rocketmq有2个节点，一个brokerName是broker-0，另一个brokerName是broker-1

## 1.启动mongodb
```
docker run \
    --publish=27017:27017  --name mongo1\
    mongo \
    --replSet rs0
    
docker exec -it mongo1 bash

cat << EOF > config-data.js 
db = db.getSiblingDB("quickstart");
db.createCollection("source");
db.createCollection("sink");
EOF


cat << EOF > config-replica.js 
rsconf = {
  _id: "rs0",
  members: [{ _id: 0, host: "127.0.0.1:27017", priority: 1.0 }],
};
rs.initiate(rsconf);
rs.status();
EOF


mongosh  mongodb://127.0.0.1:27017 config-replica.js && sleep 10 && mongosh mongodb://127.0.0.11:27017 config-data.js

```


## 2.mongo source connector
```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/mongo-source -d '{
	"connector.class": "org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSourceConnector",
	"connect.topicname":"mongoTopic",
	"kafka.connector.configs":{
		"connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
		"plugin.path": "/kafka-connecor-plugins",
		"connection.uri": "mongodb://127.0.0.1:27017/?replicaSet=rs0",
		"database": "quickstart",
		"collection": "sampleData",
		"pipeline": "[{\"$match\": {\"operationType\": \"insert\"}}, {$addFields : {\"fullDocument.travel\":\"MongoDB Kafka Connector\"}}]"
	}
}'
```

## 3.mongo sink connector
```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/mongo-sink -d '{
   "connector.class": "org.apache.rocketmq.connect.kafka.connector.KafkaRocketmqSinkConnector",
   	"connect.topicnames": "mongoTopic",
   	"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
   	"plugin.path": "/kafka-connecor-plugins/mongodb-connector",
   	"topics": "mongoTopic",
	"rocketmq.recordPartition.kafkaTopicPartition.mapper": "assignEncodedPartition",
	"assignEncodedPartition.assignments": "broker-0:0,broker-1:1",
   	"connection.uri": "mongodb://127.0.0.1:27017/?replicaSet=rs0",
   	"database": "quickstart",
   	"collection": "topicData",
   	"change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler"
   }'
```


## 4.测试
```
docker exec -it mongo1 bash

mongosh mongodb://127.0.0.1:27017/?replicaSet=rs0

use quickstart

db.sampleData.insertOne({"hello":"world"})

db.topicData.find()

```


更多关于connector：

https://www.mongodb.com/docs/kafka-connector/current/