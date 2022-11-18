##### ElasticsearchSourceConnector fully-qualified name
org.apache.rocketmq.connect.elasticsearch.connector.ElasticsearchSourceConnector

**elasticsearch-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/elasticsearchSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.elasticsearch.connector.ElasticsearchSourceConnector",
    "elasticsearchHost":"localhost",
    "elasticsearchPort":9200,
    "index":{
        "aolifu_connect": {
            "primaryShards":1,
            "id":1
        }
    },
    "max.tasks":1,
    "connect.topicname":"esTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

**elasticsearch-sink-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/elasticsearchSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.elasticsearch.connector.ElasticsearchSinkConnector",
    "elasticsearchHost":"localhost",
    "elasticsearchPort":9200,
    "max.tasks":1,
    "connect.topicnames":"esTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

parameter | effect                                                                                                                                                    | required |default
---|-----------------------------------------------------------------------------------------------------------------------------------------------------------|----------| ---
elasticsearchHost | The Host of the Elasticsearch server                                                                                                                      | yes      | null
elasticsearchPort | The Port of the Elasticsearch server                                                                                                                      | yes      |  null
index| The info of the index                                                                                                                                     | yes      | null