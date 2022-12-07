# rocketmq-connect-deltalake

## rocketmq-connect-deltalake 打包
```
mvn clean install -DskipTest -U 
```
将target目录下打包的rocketmq-connect-deltalake-0.0.1-SNAPSHOT-jar-with-dependencies.jar拷贝到connector-runtime connect.conf配置的connector-plugin目录下。
## 目前安装会遇到的问题

目前的rocketmq-connect-deltalake 使用的是2.0.0版本的deltalake.

## rocketmq-connect-deltalake 启动

首先，需要启动connect-runtime，参考rocketmq-connect-runtime的run_work.sh脚本。
* **deltalake-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-deltalake-sink-connector-name}
?config='{"connector-class":"org.apache.rocketmq.connect.deltalake.connector.DeltalakeSinkConnector","topicNames":"topicc","tablePath":"file:///Users/osgoo/Downloads/usertable","tableName":"deltatable","source-record-converter":"org.apache.rocketmq.connect.runtime.converter.RocketMQConverter","source-rocketmq":"127.0.0.1:9876","src-cluster":"DefaultCluster","refresh-interval":"10000","schemaPath":"/Users/osgoo/Downloads/user.avsc"\}'
```
启动成功会打印如下日志：
```
2021-09-06 16:23:14 INFO pool-2-thread-1 - Open HoodieJavaWriteClient successfully
```
>**注：** `rocketmq-deltalake-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-deltalake 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-deltalake-connector-name}/stop
```

## rocketmq-connect-deltalake 参数说明
* **deltalake-sink-connector 参数说明**

参数 | 类型 | 是否必须 | 描述 | 样例
|---|---|---|---|---|
|connector-class | String | 是 | sink connector类 | DeltalakeSinkConnector|
|tablePath | String | 是 | sink到deltalake的表路径 | file:///Users/osgoo/Downloads/usertable |
|tableName | String | 是 | sink到deltalake的表名称| deltatopic |
|topicNames | String | 是 | rocketmq默认每一个数据源中的表对应一个名字，该名称需和数据库表名称相同 | deltatopic |
|task-divide-strategy | Integer | 否 | task 分配策略, 默认值为 0，表示按照topic分配任务，每一个table便是一个topic | 0 |
|task-parallelism | Integer | 否 | task parallelism，默认值为 1，表示将topic拆分为多少个任务进行执行 | 2 |
|source-cluster | String | 是 | sink 端 RocketMQ cluster 名称 | DefaultCluster |
|source-rocketmq | String | 是 | sink 端获取路由信息连接到的 RocketMQ nameserver 地址 | 127.0.0.1:9876 |
|source-record-converter | String | 是 | source data 解析 | org.apache.rocketmq.connect.runtime.converter.RocketMQConverter |
|refresh-interval | String | 否 | sink的刷新时间，单位ms | 10000 |
|schemaPath | String | 是 | sink的schema地址 | /Users/osgoo/Downloads/user.avsc" |


示例配置如下
```js
{
	"connector-class": "org.apache.rocketmq.connect.deltalake.connector.DeltalakeSinkConnector",
	"topicNames": "topicc",
	"tablePath": "file:///Users/osgoo/Downloads/usertable",
	"tableName": "deltatable",
	"source-record-converter": "org.apache.rocketmq.connect.runtime.converter.RocketMQConverter",
	"source-rocketmq": "127.0.0.1:9876",
	"source-cluster": "DefaultCluster",
	"refresh-interval": "10000",
	"schemaPath": "/Users/osgoo/Downloads/user.avsc"
}
```

## 在本地Mac系统将RocketMQ的topic数据导入到DeltaLake表中demo
涉及到的组件：rocketmq、rocketmq-connector-runtime、rocketmq-connect-deltalake、hdfs、avro、spark-shell
### 0、启动hdfs
下载hadoop包，https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz
cd /Users/osgoo/Documents/hadoop-2.10.1
vi core-site.xml
<configuration>
<property>
 <name>fs.defaultFS</name>
 <!-- 可以通过命令hostname 查看主机名字  这里的主机名字是hadoop1-->
 <value>hdfs://localhost:9000</value>
</property>
<!--覆盖掉core-default.xml中的默认配置-->
</configuration>

vi hdfs-site.xml
<configuration>
<property>
        <name>dfs.replication</name>
        <value>1</value>
  </property>
</configuration>

./bin/hdfs namenode -format
./sbin/start-dfs.sh 
jps 看下namenode,datanode
lsof -i:9000
./bin/hdfs dfs -mkdir -p /Users/osgoo/Downloads

### 1、启动rocketmq集群，创建rocketmq-connector内置topic
QickStart： https://rocketmq.apache.org/docs/quick-start/
sh mqadmin updatetopic -t connector-cluster-topic -n localhost:9876 -c DefaultCluster
sh mqadmin updatetopic -t connector-config-topic -n localhost:9876 -c DefaultCluster
sh mqadmin updatetopic -t connector-offset-topic -n localhost:9876 -c DefaultCluster
sh mqadmin updatetopic -t connector-position-topic -n localhost:9876 -c DefaultCluster

### 2、创建数据入湖的源端topic，deltatopic
sh mqadmin updatetopic -t deltatopic -n localhost:9876 -c DefaultCluster

### 3、编译rocketmq-connect-deltalake-0.0.1-SNAPSHOT-jar-with-dependencies.jar
cd rocketmq-connect-deltalake
mvn clean install -DskipTest -U

### 4、启动rocketmq-connector runtime
配置connect.conf
--------------
workerId=DEFAULT_WORKER_1
storePathRootDir=/Users/osgoo/Downloads/storeRoot

\# Http port for user to access REST API
httpPort=8082

\# Rocketmq namesrvAddr
namesrvAddr=localhost:9876

\# Source or sink connector jar file dir,The default value is rocketmq-connect-sample
pluginPaths=/Users/osgoo/Downloads/connector-plugins
---------------
拷贝 rocketmq-connect-deltalake-0.0.1-SNAPSHOT-jar-with-dependencies.jar 到 pluginPaths=/Users/osgoo/Downloads/connector-plugins

sh run_worker.sh

### 5、配置入湖config
curl http://localhost:8082/connectors/rocketmq-connect-deltalake?config='\{"conclass":"org.apache.rocketmq.connect.deltalake.connector.DeltalakeSinkConnector","task-class":"org.apache.rocketmq.connect.deltalake.connector.DeltalakeSinkTask","topicNames":"deltatopic","connect-topicname":"deltatopic","tablePath":"hdfs://localhost:9000/Users/osgoo/Downloads/usertable","tableName":"deltatable","source-record-converter":"org.apache.rocketmq.connect.runtime.converter.RocketMQConverter","source-rocketmq":"127.0.0.1:9876","source-cluster":"DefaultCluster","refresh-interval":"10000","schemaPath":"/Users/osgoo/Downloads/user.avsc"\}'

### 6、发送消息到deltatopic
```java

File s = new File("/Users/osgoo/Downloads/user.avsc");
Schema schema = new Schema.Parser().parse(s);

GenericRecord user1 = new GenericData.Record(schema);
user1.put("name", "osgoo");
user1.put("favorite_number", 256);
user1.put("favorite_color", "white");


ByteArrayOutputStream bao = new ByteArrayOutputStream();
GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(schema);
Encoder e = EncoderFactory.get().jsonEncoder(schema, bao);
w.write(user1, e);
e.flush();

bao.toByteArray();
```


### 7、 利用spark读取

cd /Users/osgoo/Downloads/spark-3.1.2-bin-hadoop3.2/bin

bin/spark-shell --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

使用python读取数据
```py
import pyspark
from delta import *

df = spark.read.format("delta").load("/tmp/delta-table")
df.show()
```
使用python查询数据并做聚合
```py
from delta.tables import *

deltaTablePeopleUpdates = DeltaTable.forPath(spark, 'file:///Users/osgoo/Downloads/usertable')
deltaTablePeople = DeltaTable.forPath(spark, 'file:///Users/osgoo/Downloads/usertable-updates1')

dfUpdates = deltaTablePeopleUpdates.toDF()


deltaTablePeople.alias("user").merge(
    dfUpdates.alias("updates"),
    "user.name = updates.name") \
  .whenNotMatchedInsertAll() \
  .execute()
  
deltaTablePeople.alias('user') \
  .merge(
    dfUpdates.alias('updates'),
    'user.name = updates.name'
  ) \
  .whenMatchedUpdate(set =
    {
      "name": "updates.name",
      "favorite_number": "updates.favorite_number",
      "favorite_color": "updates.favorite_color"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "name": "updates.name",
      "favorite_number": "updates.favorite_number",
      "favorite_color": "updates.favorite_color"
    }
  ) \
  .execute()
```

