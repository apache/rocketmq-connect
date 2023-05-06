##### ElasticsearchSourceConnector fully-qualified name
org.apache.rocketmq.connect.elasticsearch.connector.ElasticsearchSourceConnector

**elasticsearch-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/elasticsearchSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.clickhouse.source.ClickHouseSourceConnector",
    "clickhousehost":"120.48.26.195",
    "clickhouseport":8123,
    "clickhousedatabase":"default",
    "username":"default",
    "password":"123456",
    "table":"tableName",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

```
POST  http://localhost:8082/connectors/clickhouseSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.clickhouse.source.ClickHouseSourceConnector",
    "clickhousehost":"120.48.26.195",
    "clickhouseport":8123,
    "database":"default",
    "username":"default",
    "password":"123456",
    "table":"tableName",
    "topic":"topic",
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