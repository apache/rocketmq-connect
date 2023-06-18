# rocketmq-connect-jdbc

为方便扩展，rocketmq-connect-jdbc目前采用Spi插件的形式进行扩展，核心扩展api主要有：
   ```SPI api
   org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectFactory
   org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect
   ```
目前支持Mysql、OpenMLDB数据库, pg、oracle、sqlserver、db2 等关系型数据库还在持续扩展中

## rocketmq-connect-jdbc使用方法

1. 进入想要使用的connectors目录下（以rocketmq-connect-jdbc目录为例），使用以下指令将插件进行打包
   ```shell
   mvn clean package -Dmaven.test.skip=true
   ```
2. 打包好的插件以tar.gz的模式出现在`rocketmq-connect-jdbc/target/`目录下

3. 在`distribution/conf`目录下找的对应的配置文件进行更新，对于standalone的启动方式，更新`connect-standalone.conf`文件中的`pluginPaths`变量
   
   ```lombok.config
   pluginPaths=(you plugin path)
   ```
   
   相应的，使用distributed启动方式，则更新`connect-distributed.conf`中的变量
4. 在源数据库和目的数据库准备好需要使用的表，可参考[RocketMQ Connect实战1](https://rocketmq.apache.org/zh/docs/%e6%95%b0%e6%8d%ae%e9%9b%86%e6%88%90/28RocketMQ%20Connect%20In%20Action1/)
5. 创建并启动对应的`SourceConnector`以及`SinkConnector`



## 示例：jdbc -> Rocketmq -> doris

注: 目前支持将根据mysql ,openMLDB对应的source connect以流式的方式导入到rocketmq中的数据导入到doris

### 打包connectors并配置conf文件

```
mvn clean package -Dmaven.test.skip=true
```

### 启动connector

* **jdbc-source-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-source-connector-name}
{
    "connector.class":"org.apache.rocketmq.connect.jdbc.mysql.source.JdbcSourceConnector",
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

* **doris-sink-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-sink-connector-name}
{
    "connector.class":"org.apache.rocketmq.connect.doris.connector.DorisSinkConnector",
    "max-task":"1",
    "table.whitelist":"sink_test.doris_test_sink",
    "connect.topicname":"doris_test_sink",
    "connect.topicnames":"doris_test_sink",
    "host":"xx.xx.xx.xx",
    "port":"xxxx",
    "user":"xx",
    "passwd":"****",
    "database":"database",
    "insert.mode":"INSERT",
    "db.timezone":"UTC",
    "table.types":"TABLE",
    "auto.create":"true",
    "source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"}'
}
```

> **注：** `rocketmq-jdbc-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

### rocketmq-connect-jdbc 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-connector-name}/stop
```

### rocketmq-connect-jdbc 参数说明

* **jdbc-source-connector 参数说明**

| KEY                      | TYPE    | Must be filled | Description      | Example                                                   |
|--------------------------| ------- | -------------- |------------------| --------------------------------------------------------- |
| connection.url           | String  | YES            | source端 jdbc连接   | jdbc:mysql://XXXXXXXXX:3306                               |
| connection.user          | String  | YES            | source端 DB 用户名   | root                                                      |
| connection.password      | String  | YES            | source端 DB 密码    | root                                                      |
| connection.attempts      | String  | YES            | source端 DB连接重试次数 | 3                                                         |
| connection.backoff.ms    | Long    | YES            |                  |                                                           |
| poll.interval.ms         | Long    | YES            | 拉取间隔时间           | 3000ms                                                    |
| batch.max.rows           | Integer | NO             | 每次拉取数量           | 300                                                       |
| mode                     | Integer | NO             | 拉取模式             | bulk、timestamp、incrementing、timestamp+incrementing        |
| incrementing.column.name | Integer | NO             | 增量字段，常用ID        | id                                                        |
| timestamp.column.name    | String  | YES            | 时间增量字段           | modified_time                                             |
| table.whitelist          | String  | YES            | 需要扫描的表           | db.table,db.table01                                       |
| max.tasks                | Integer | YES            | 任务数量，最大不能大于表的数量  | 2                                                         |
| key.converter            | Integer | YES            | key转换器           | org.apache.rocketmq.connect.doris.converter.JsonConverter |
| value.converter          | Integer | YES            | data转换器          | org.apache.rocketmq.connect.doris.converter.JsonConverter |

```
注：1.source拉取的数据写入到以表名自动创建的topic中，如果需要写入特定的topic中则需要指定"connect-topicname" 参数
   2.topic.prefix参数可以为自动创建的topic增加前缀，用来进行逻辑的隔离
```

* **jdbc-sink-connector 参数说明**

| KEY                 | TYPE    | Must be filled | Description     | Example                                                   |
|---------------------| ------- | -------------- |-----------------| --------------------------------------------------------- |
| connection.url      | String  | YES            | sink端 jdbc连接    | jdbc:mysql://XXXXXXXXX:3306                               |
| connection.user     | String  | YES            | sink端 DB 用户名    | root                                                      |
| connection.password | String  | YES            | sink端 DB 密码     | root                                                      |
| host                | String  | YES            | doris host      | 192.168.0.1                                               |
| port                | String  | YES            | doris http port | 8030                                                      |
| user                | String  | YES            | 监听的topic        | root                                                      |
| passwd              | String  | YES            | 监听的topic        | passwd                                                    |
| max.tasks           | Integer | NO             | 任务数量            | 2                                                         |
| key.converter       | Integer | YES            | key转换器          | org.apache.rocketmq.connect.doris.converter.JsonConverter |
| value.converter     | Integer | YES            | data转换器         | org.apache.rocketmq.connect.doris.converter.JsonConverter |
