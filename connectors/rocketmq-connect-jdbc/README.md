# rocketmq-connect-jdbc

```  
注: 目前支持的数据库类型为 mysql ,openMLDB ，其它数据库的jdbc模式持续扩展中
```  

## rocketmq-connect-jdbc 打包

```
mvn clean package -Dmaven.test.skip=true
```

## rocketmq-connect-jdbc 启动

* **jdbc-source-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-source-connector-name}
{
    "connector.class":"org.apache.rocketmq.connect.jdbc.connector.JdbcSourceConnector",
    "max.tasks":"2",
    "connection.url":"jdbc:mysql://XXXXXXXXX:3306",
    "connection.user":"*****",
    "connection.password":"*****",
    "table.whitelist":"db.table",
    "mode": "incrementing",
    "incrementing.column.name":"id",
    "timestamp.initial": -1,
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

* **jdbc-sink-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-sink-connector-name}
{
    "connector-class":"org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConnector",
    "max-task":"2",
    "connect-topicname":"targetTopic",
    "connection.url":"jdbc:mysql://*****:3306/{sinkDbName}",
    "connection.user":"******",
    "connection.password":"******",
    "pk.fields":"id",
    "pk.mode":"record_value",
    "insert.mode":"UPSERT",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter"
}
```

> **注：** `rocketmq-jdbc-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-jdbc 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-connector-name}/stop
```

## rocketmq-connect-jdbc 参数说明

* **jdbc-source-connector 参数说明**

| KEY                      |  TYPE   | Must be filled | Description      | Example                                                     
|--------------------------|----|---------|------------------|-------------------------------------------------------------|
| connection.url           | String  | YES           | source端 jdbc连接   | jdbc:mysql://XXXXXXXXX:3306                                 |
| connection.user          | String  | YES           | source端 DB 用户名   | root                                                        |
| connection.password      | String  | YES           | source端 DB 密码    | root                                                        |
| connection.attempts      | String  | YES           | source端 DB连接重试次数 | 3                                                           |
| connection.backoff.ms    | Long    | YES           |                  |
| poll.interval.ms         | Long    | YES           | 拉取间隔时间           | 3000ms                                                      |
| batch.max.rows           | Integer | NO            | 每次拉取数量           | 300                                                         |
| mode                     | Integer | NO            | 拉取模式             | bulk、timestamp、incrementing、timestamp+incrementing          |
| incrementing.column.name | Integer | NO            | 增量字段，常用ID        | id                                                          |
| timestamp.column.name    | String  | YES           | 时间增量字段           | modified_time                                               |
| table.whitelist          | String  | YES           | 需要扫描的表           | db.table,db.table01                                         |
| max.tasks                | Integer | YES           | 任务数量，最大不能大于表的数量  | 默认是1                                                        |
| value.converter          | Integer | YES           | value转换器         | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter |
| key.converter            | Integer | YES           | key转换器           | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter |

```  
注：1.source拉取的数据写入到以表名自动创建的topic中，如果需要写入特定的topic中则需要指定"connect-topicname" 参数
   2.topic.prefix参数可以为自动创建的topic增加前缀，用来进行逻辑的隔离
```  

* **jdbc-sink-connector 参数说明**

| KEY                     |  TYPE   | Must be filled | Description| Example
|-------------------------|----|---------|---------------|------------------|
| connection.url          | String  | YES           | sink端 jdbc连接          | jdbc:mysql://XXXXXXXXX:3306|
| connection.user         | String  | YES           | sink端 DB 用户名 | root |
| connection.password     | String  | YES           | sink端 DB 密码   | root |
| connection.attempts     | String  | NO           | sink端 DB连接重试次数 | 3 |
| connection.backoff.ms   | Long    | NO           |  |
| connect-topicname       | Long    | YES          |监听的topic  | topic-name |
| pk.fields               | String  | NO           |写入侧主键配置，用于更新使用 | id |
| pk.mode                 | String  | NO           |获取主键的模式 | none、record_value |
| insert.mode             | Integer | YES           |写入模式 | UPDATE、UPSERT、INSERT |
| max.tasks               | Integer | NO           |任务数量 | 2 |
| value.converter          | Integer | YES           | value转换器         | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter |
| key.converter            | Integer | YES           | key转换器           | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter |

```  
注: openMLDB maven包的引入：
---------MacOS 系统下运行-------------
     <dependency>
            <groupId>com.4paradigm.openmldb</groupId>
            <artifactId>openmldb-native</artifactId>
            <version>0.5.0-macos</version>
        </dependency>
        <dependency>
            <groupId>com.4paradigm.openmldb</groupId>
            <artifactId>openmldb-jdbc</artifactId>
            <version>0.5.0</version>
            <exclusions>
                <exclusion>
                    <groupId>com.4paradigm.openmldb</groupId>
                    <artifactId>openmldb-native</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
---------Linux 系统下运行-------------
        <dependency>
            <groupId>com.4paradigm.openmldb</groupId>
            <artifactId>openmldb-jdbc</artifactId>
            <version>0.5.0</version>
        </dependency>
```
