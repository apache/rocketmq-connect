# DEMO

MySQL Source(CDC)  -  >RocketMQ Connect  ->  MySQL Sink(JDBC)

## 准备

### 启动RocketMQ

1. Linux/Unix/Mac
2. 64bit JDK 1.8+;
3. Maven 3.2.x或以上版本;
4. 启动 [RocketMQ](https://rocketmq.apache.org/docs/quick-start/);



**tips** : ${ROCKETMQ_HOME} 位置说明

>bin-release.zip 版本：/rocketmq-all-4.9.4-bin-release
>
>source-release.zip 版本：/rocketmq-all-4.9.4-source-release/distribution


### 启动Connect


#### Connector插件编译

Debezium RocketMQ Connector
```
$ cd rocketmq-connect/connectors/rocketmq-connect-debezium/
$ mvn clean package -Dmaven.test.skip=true
```

将 Debezium MySQL RocketMQ Connector 编译好的包放入Runtime加载目录。命令如下：
```
mkdir -p /usr/local/connector-plugins
cp rocketmq-connect-debezium-mysql/target/rocketmq-connect-debezium-mysql-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/local/connector-plugins
```

JDBC Connector

将 JDBC Connector 编译好的包放入Runtime加载目录。命令如下：
```
$ cd rocketmq-connect/connectors/rocketmq-connect-jdbc/
$ mvn clean package -Dmaven.test.skip=true
cp rocketmq-connect-jdbc/target/rocketmq-connect-jdbc-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/local/connector-plugins

```

#### 启动Connect Runtime
```
cd  rocketmq-connect

mvn -Prelease-connect -DskipTests clean install -U

```

修改配置`connect-standalone.conf` ，重点配置如下
```
$ cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT
$ vim conf/connect-standalone.conf
```

```
workerId=standalone-worker
storePathRootDir=/tmp/storeRoot

## Http port for user to access REST API
httpPort=8082

# Rocketmq namesrvAddr
namesrvAddr=localhost:9876

# RocketMQ acl
aclEnable=false
accessKey=rocketmq
secretKey=12345678

autoCreateGroupEnable=false
clusterName="DefaultCluster"

# 核心配置，将之前编译好debezium包的插件目录配置在此；
# Source or sink connector jar file dir,The default value is rocketmq-connect-sample
pluginPaths=/usr/local/connector-plugins
```


```
cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT

sh bin/connect-standalone.sh -c conf/connect-standalone.conf &

```

### MySQL镜像
使用debezium的MySQL docker搭建环境MySQL数据库
```
docker run -it --rm --name MySQL -p 3306:3306 -e MySQL_ROOT_PASSWORD=debezium -e MySQL_USER=MySQLuser -e MySQL_PASSWORD=MySQLpw quay.io/debezium/example-MySQL:1.9
```
MySQL信息

端口：3306

账号：root/debezium

slave:debezium/dbz


### 测试数据

通过root/debezium账号登录数据库

源数据库表：inventory.employee

```
CREATE database inventory;

use inventory;
CREATE TABLE `employee` (
`id` bigint NOT NULL AUTO_INCREMENT,
`name` varchar(128) DEFAULT NULL,
`howold` int DEFAULT NULL,
`male` int DEFAULT NULL,
`company` varchar(128) DEFAULT NULL,
`money` double DEFAULT NULL,
`begin_time` datetime DEFAULT NULL,
`modify_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`decimal_test` decimal(11,2) DEFAULT NULL COMMENT 'test decimal type',
PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8;



INSERT INTO `employee` VALUES (1, 'name-01', 24, 6, 'company', 9987, '2021-12-22 08:00:00', '2022-06-14 18:20:11', 321.11);
INSERT INTO `employee` VALUES (2, 'name-02', 19, 7, 'company', 32232, '2021-12-29 08:00:00', '2022-06-14 18:18:47', 77.12);
INSERT INTO `employee` VALUES (8, 'name-03', 20, 1, NULL, 0, NULL, '2022-06-14 18:26:05', 11111.00);
INSERT INTO `employee` VALUES (9, 'name-04', 21, 1, 'company', 12345, '2021-12-24 20:44:10', '2022-06-14 18:20:02', 123.12);
INSERT INTO `employee` VALUES (11, 'name-05', 50, 2, 'company', 33333, '2021-12-24 22:14:52', '2022-06-14 18:19:58', 123.12);
INSERT INTO `employee` VALUES (12, 'name-06', 19, 3, NULL, 0, NULL, '2022-06-14 18:26:12', 111233.00);
INSERT INTO `employee` VALUES (13, 'name-07', 20, 4, 'company', 3237, '2021-12-29 01:31:03', '2022-06-14 18:19:27', 52.00);
INSERT INTO `employee` VALUES (14, 'name-08', 25, 15, 'company', 32255, '2022-02-08 19:06:39', '2022-06-14 18:18:32', 0.00);
INSERT INTO `employee` VALUES (15, NULL, 0, 0, NULL, 0, NULL, '2022-06-14 20:13:29', NULL);




use 
```

目标库：inventory_2.employee
```
CREATE database inventory_2;
use inventory_2;
CREATE TABLE `employee` (
`id` bigint NOT NULL AUTO_INCREMENT,
`name` varchar(128) DEFAULT NULL,
`howold` int DEFAULT NULL,
`male` int DEFAULT NULL,
`company` varchar(128) DEFAULT NULL,
`money` double DEFAULT NULL,
`begin_time` datetime DEFAULT NULL,
`modify_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
`decimal_test` decimal(11,2) DEFAULT NULL COMMENT 'test decimal type',
PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8;
```

## 启动Connector

### 启动Debezium source connector

同步原表数据：inventory.employee
作用：通过解析MySQL binlog 封装成通用的ConnectRecord对象，发送的RocketMQ Topic当中

```
curl-X POST-H"Content-Type: application/json"http: //127.0.0.1:8082/connectors/MySQLCDCSource'{
"connector.class": "org.apache.rocketmq.connect.debezium.MySQL.DebeziumMySQLConnector",
"max.task": "1",
"connect.topicname": "debezium-MySQL-source-topic",
"kafka.transforms": "Unwrap",
"kafka.transforms.Unwrap.delete.handling.mode": "none",
"kafka.transforms.Unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"kafka.transforms.Unwrap.add.headers": "op,source.db,source.table",
"database.history.skip.unparseable.ddl": true,
"database.history.name.srv.addr": "localhost:9876",
"database.history.rocketmq.topic": "db-history-debezium-topic",
"database.history.store.only.monitored.tables.ddl": true,
"include.schema.changes": false,
"database.server.name": "dbserver1",
"database.port": 3306,
"database.hostname": "数据库ip",
"database.connectionTimeZone": "UTC",
"database.user": "debezium",
"database.password": "dbz",
"table.include.list": "inventory.employee",
"max.batch.size": 50,
"database.include.list": "inventory",
"snapshot.mode": "when_needed",
"database.server.id": "184054",
"key.converter": "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
"value.converter": "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}'
```

### 启动 jdbc sink connector

作用：通过消费Topic中的数据，通过JDBC协议写入到目标表当中

```
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/jdbcmysqlsinktest -d '{
  "connector.class": "org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConnector",
  "max.task": "2",
  "connect.topicnames": "debezium-mysql-source",
  "connection.url": "jdbc:mysql://数据库ip:3306/inventory_2",
  "connection.user": "root",
  "connection.password": "debezium",
  "pk.fields": "id",
  "table.name.from.header": "true",
  "pk.mode": "record_key",
  "insert.mode": "UPSERT",
  "db.timezone": "UTC",
  "table.types": "TABLE",
  "errors.deadletterqueue.topic.name": "dlq-topic",
  "errors.log.enable": "true",
  "errors.tolerance": "ALL",
  "delete.enabled": "true",
  "key.converter": "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
  "value.converter": "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}'
```


以上两个Connector任务创建成功以后
通过root/debezium账号登录数据库

对源数据库表：inventory.employee增删改
即可同步到目标办inventory_2.employee


