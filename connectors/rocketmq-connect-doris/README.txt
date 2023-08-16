# rocketmq-connect-doris

## rocketmq-connect-doirs packaging

```
mvn clean package -Dmaven.test.skip=true
```

## rocketmq-connect-doris
Supporting for database languages:
|   Language     |  Support   |
|----------------|------------|
|   DDL          |  NO        |
|   DML          |  YES       |
|   DCL          |  NO        |
|   TCL          |  NO        |
DDL:
Currently, sink-connect-doris load data to Doris via Stream Load which does not support DDL, so DDL
 is also not supported by sink-connect-doris.
DML:
Sink-connect-doris only support insert mode, and entries should have increment filed.
DCL:
Not support now.
TCL:
Transaction concept not different in Mysql and Doris, so TCL is not supported.

## How to use
0. Doris cluster and Rocketmq Cluster should be prepared.
1. build rocketmq connect (https://github.com/apache/rocketmq-connect).
2. package rocketmq-connect-jdbc and rocketmq-connect-doris.
3. run rocketmq connect runtime.
4. create source-connect-mysql.
5. create sourcr-connect-doris.

```

* **doris-sink-connector** Start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-sink-connector-name}
{
    "connector.class":"org.apache.rocketmq.connect.doris.connector.DorisSinkConnector",
    "max.tasks":"1",
    "table.whitelist":"sink_test.doris_test_sink",
    "connect.topicnames":"doris_test_sink",
    "host":"xx.xx.xx.xx",
    "port":"xxxx",
    "user":"xx",
    "passwd":"****",
    "database":"database",
    "insert.mode":"INSERT",
    "db.timezone":"UTC",
    "table.types":"TABLE",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"}'
}
```

> **Note:
1.rocketmq-jdbc-connect depends on the startup of the `rocketmq-connect-runtime` project. You will need to put all the `jar` packages into the path configured by `pluginPaths` in the `runtime` project and re-execute the above startup request. This value is configured in the `connect.conf` file under the `runtime` project.
2.The data pulled by the source connector is written to the topic automatically created with the table name. If you need to write to a specific topic, you need to specify the "connect-topicname" parameter
3.The table name used by sink-connect-doris is specified by schema in message, which is the same with original mysql table name, so doris table name created should be the same with mysql table name.

## rocketmq-connect-jdbc Stop

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-doris-connector-name}/stop
```

* **jdbc-sink-connector 参数说明**

|         KEY                 |  TYPE   | Must be filled | Description      | Example
|------------------------|----|---------|---------------|-------------------|
|host                         | String  | YES           |doris host         | 192.168.0.1   |
|port                         | String  | YES           |doris http port    | 8030          |
|user                         | String  | YES           |doris user name    | root          |
|passwd                       | String  | YES           |doris passwd       | passwd        |
|max.tasks                     | Integer | NO            |task number        | 2             |
|key.converter                | String  | YES           |data converter     | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter          |
|value.converter              | String  | YES           |value converter    | org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter          |
```

## Quick Start
Doris Cluster:
Fe: 192.168.0.1
Be: 192.168.0.2 192.168.0.3

Rocketmq Cluster:
Nameserver: 192.168.0.1
Broker-a:   192.168.0.2
Broker-b:   192.168.0.3

Note: run rocketmq-connect and source mysql on 192.168.0.3

1. create mysql source table and insert entries;
```
ssh root@192.168.0.3
mysql -uroot –p
use sink_test;
DROP TABLE IF EXISTS `doris_test_sink`;
CREATE TABLE `doris_test_sink` (
`id` int not NULL AUTO_INCREMENT COMMENT "",
`number` int NULL COMMENT "",
`price` double NULL COMMENT "",
`skuname` varchar(40) NULL COMMENT "",
`skudesc` varchar(200) NULL COMMENT "",
PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8;
BEGIN;
INSERT INTO `doris_test_sink` VALUES (10001, 12, 13.3, 'test1','this is atest');
INSERT INTO `doris_test_sink` VALUES (10002 ,100, 15.3, 'test2', 'this is atest');
INSERT INTO `doris_test_sink` VALUES (10003, 102, 16.3, 'test3', 'this is atest');
INSERT INTO `doris_test_sink` VALUES (10004, 120, 17.3, 'test4', 'this is atest');
COMMIT;
select * from doris_test_sink;
```

2. create Doris table
```
ssh root@192.168.0.1
mysql -uroot -p
use db_1;
DROP TABLE IF EXISTS `doris_test_sink`;
CREATE TABLE `doris_test_sink` (
  `id` int not NULL COMMENT "",
  `number` int NULL COMMENT "",
  `price` double NULL COMMENT "",
  `skuname` varchar(40) NULL COMMENT "",
  `skudesc` varchar(200) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "V2"
);
select * from doris_test_sink;
```

3. run Rocketmq Connect
```
ssh root@192.168.0.3
cd rocketmq-connect
builde rocketmq-connect
mvn -Prelease-connect -DskipTests clean install –U
```
build rocketmq-connect-jdbc
build rocketmq-connect-doris
modify connect-standalone.conf specialize nameserver ip and plugins path
```
cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT
sh bin/connect-standalone.sh -c path/to/connect-standalone.conf
```
4. create source and sink connector
```
ssh root@192.168.0.3
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/jdbc-mysql-source-test -d  '{"connector.class":"org.apache.rocketmq.connect.jdbc.mysql.source.MysqlJdbcSourceConnector","max.tasks":"1","connect.topicname":"doris_test_sink","connection.url":"jdbc:mysql://127.0.0.1:3306","connection.user":"****","connection.password":"*****","table.whitelist":"sink_test.doris_test_sink","mode": "incrementing","incrementing.column.name":"id","key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter","value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"}'
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/jdbc-doris-sink-test -d '{"connector.class":"org.apache.rocketmq.connect.doris.connector.DorisSinkConnector","max.tasks":"1","table.whitelist":"sink_test.doris_test_sink","connect.topicname":"doris_test_sink","connect.topicnames":"doris_test_sink","host":"192.168.0.1","port":"7030","user":"****","passwd":"*****","database":"db_1","insert.mode":"INSERT","db.timezone":"UTC","table.types":"TABLE","auto.create":"true","source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter","key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter","value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"}'
```

5. verify result on Doris
```
ssh root@192.168.0.1
mysql –uroot -p
use db_1;
select * from doris_test_sink;
```

