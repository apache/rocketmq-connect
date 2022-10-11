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
    "connect.topicname":"configInfo",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.StringConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.StringConverter"
}
```

##### parameter configuration

parameter | effect                                                                                                                                                    | required |default
---|-----------------------------------------------------------------------------------------------------------------------------------------------------------|----------| ---
elasticsearchHost | The Host of the Elasticsearch server                                                                                                                      | yes      | null
elasticsearchPort | The Port of the Elasticsearch server                                                                                                                      | yes      |  null
index| The info of the index                                                                                                                                     | yes      | null