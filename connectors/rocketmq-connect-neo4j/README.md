### Neo4jSourceConnector fully-qualified name
org.apache.rocketmq.connect.neo4j.source.Neo4jSourceConnector

**neo4j-source-connector node source**
```
POST  http://${runtime-ip}:${runtime-port}/connectors/neo4jSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.neo4j.source.Neo4jSourceConnector",
    "neo4jHost":"localhost",
    "neo4jPort":7687,
    "neo4jDataBase":"test",
    "neo4jUser":"test",
    "neo4jPassword":"root123456",
    "labelType":"node",
    "topic":"nodeNeo4jTopic",
    "labels":"Goods",
    "column":[
        {
            "name":"goodsId",
            "type":"long",
            "columnType":"primaryKey",
            "valueExtract":"#{goodsId}"
        },
        {
            "name":"label",
            "type":"string",
            "columnType":"primaryLabel"
        },
        {
            "name":"goodsName",
            "type":"string",
            "columnType":"nodeProperty",
            "valueExtract":"#{goodsName}"
        }
    ],
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

**neo4j-source-connector relationship source**
```
POST  http://${runtime-ip}:${runtime-port}/connectors/neo4jSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.neo4j.source.Neo4jSourceConnector",
    "neo4jHost":"localhost",
    "neo4jPort":7687,
    "neo4jDataBase":"test",
    "neo4jUser":"test",
    "neo4jPassword":"root123456",
    "labelType":"relationship",
    "topic":"edgeNeo4jTopic",
    "labels":"order_goods",
    "column":[
        {
            "name":"orderGoodsId",
            "type":"long",
            "columnType":"primaryKey",
            "valueExtract":"#{orderGoodsId}"
        },
        {
            "name":"type",
            "type":"string",
            "columnType":"primaryLabel"
        },
        {
            "name":"orderId",
            "type":"long",
            "columnType":"srcPrimaryKey",
            "valueExtract":"#{orderId}"
        },
        {
            "name":"Order",
            "type":"string",
            "columnType":"srcPrimaryLabel"
        },
        {
            "name":"goodsId",
            "type":"long",
            "columnType":"dstPrimaryKey",
            "valueExtract":"#{goodsId}"
        },
        {
            "name":"Goods",
            "type":"string",
            "columnType":"dstPrimaryLabel"     
        },
        {
            "name":"orderGoodsTitle",
            "type":"string",
            "columnType":"relationshipProperty",
            "valueExtract":"#{orderGoodsTitle}"
        }
    ],
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

### Neo4jSinkConnector fully-qualified name
org.apache.rocketmq.connect.neo4j.sink.Neo4jSinkConnector

**neo4j-sink-connector node sink**

```
POST  http://${runtime-ip}:${runtime-port}/connectors/neo4jSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.neo4j.sink.Neo4jSinkConnector",
    "neo4jHost":"localhost",
    "neo4jPort":7687,
    "neo4jDataBase":"neo4j",
    "neo4jUser":"test",
    "neo4jPassword":"root123456",
    "labelType":"node",
    "connect.topicnames":"nodeNeo4jTopic",
    "labels":"Goods",
    "column":[
        {
            "name":"goodsId",
            "type":"long",
            "columnType":"primaryKey",
            "valueExtract":"#{goodsId}"
        },
        {
            "name":"Goods",
            "type":"string",
            "columnType":"primaryLabel",
            "valueExtract":"#{label}"
        },
        {
            "name":"goodsName",
            "type":"string",
            "columnType":"nodeProperty",
            "valueExtract":"#{goodsName}"
        }
    ],
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

**neo4j-sink-connector relationship sink**

```
POST  http://${runtime-ip}:${runtime-port}/connectors/neo4jSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.neo4j.sink.Neo4jSinkConnector",
    "neo4jHost":"localhost",
    "neo4jPort":7687,
    "neo4jDataBase":"neo4j",
    "neo4jUser":"test",
    "neo4jPassword":"root123456",
    "labelType":"relationship",
    "connect.topicnames":"edgeNeo4jTopic",
    "labels":"order_goods",
    "column":[
        {
            "name":"orderGoodsId",
            "type":"long",
            "columnType":"primaryKey",
            "valueExtract":"#{orderGoodsId}"
        },
        {
            "name":"order_goods",
            "type":"string",
            "columnType":"primaryLabel",
            "valueExtract":"#{type}"
        },
        {
            "name":"orderId",
            "type":"long",
            "columnType":"srcPrimaryKey",
            "valueExtract":"#{orderId}"
        },
        {
            "name":"Order",
            "type":"string",
            "columnType":"srcPrimaryLabel",
            "valueExtract":"#{Order}"
        },
        {
            "name":"goodsId",
            "type":"long",
            "columnType":"dstPrimaryKey",
            "valueExtract":"#{goodsId}"
        },
        {
            "name":"Goods",
            "type":"string",
            "columnType":"dstPrimaryLabel",
            "valueExtract":"#{Goods}"     
        },
        {
            "name":"orderGoodsTitle",
            "type":"string",
            "columnType":"relationshipProperty",
            "valueExtract":"#{orderGoodsTitle}"
        }
    ],
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

| parameter          | effect                                                            | required          | default |
|--------------------|-------------------------------------------------------------------|-------------------|---------|
| neo4jHost          | The Host of the neo4j server                                      | yes               | null    |
| neo4jPort          | The Port of the neo4j server                                      | yes               | null    |
| neo4jDataBase      | The database to read or write                                     | yes               | null    |
| neo4jUser          | user                                                              | yes               | null    |
| neo4jPassword      | password                                                          | yes               | null    |
| labelType          | node ro relationship                                              | yes               | null    |
| labels             | node label ro relationship type, If multiple are separated by ',' | yes               | null    |
| name               | mapper into ConnectRecord field name or from ConnectRecord to neo4j propertyKey | yes               | null    |
| type               | column mapper value type                                          | yes               | null    |
| columnType         | neo4j data type primaryKey,primaryLabel,nodeProperty,nodeJsonProperty,srcPrimaryKey,srcPrimaryLabel,dstPrimaryKey,dstPrimaryLabel,relationshipProperty,relationshipJsonProperty,| yes               | null    |
| valueExtract       | column mapper value extractor，if need extract value should start with'#{' end with '}',else value is fixed value  | no               | null    |
| topic              | RocketMQ topic for source connector to write into                 | yes (source only) | null    |
| connect.topicnames | RocketMQ topic for sink connector to read from                    | yes (sink only)   | null    |



参考文档:
https://neo4j.com/docs/cypher-manual/current/clauses
https://github.com/neo4j-contrib/neo4j-streams
https://github.com/alibaba/DataX
