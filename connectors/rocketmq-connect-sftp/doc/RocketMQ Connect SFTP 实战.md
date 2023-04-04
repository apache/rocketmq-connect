# RocketMQ Connect SFTP 实战

## 准备

### 启动RocketMQ

1. Linux/Unix/Mac
2. 64bit JDK 1.8+;
3. Maven 3.2.x或以上版本;
4. 启动 [RocketMQ](https://rocketmq.apache.org/docs/quick-start/);



**提示** : ${ROCKETMQ_HOME} 位置说明

>bin-release.zip 版本：/rocketmq-all-4.9.4-bin-release
>
>source-release.zip 版本：/rocketmq-all-4.9.4-source-release/distribution


### 启动Connect


#### Connector插件编译

RocketMQ Connector SFTP
```
$ cd rocketmq-connect/connectors/rocketmq-connect-sftp/
$ mvn clean package -Dmaven.test.skip=true
```

将  RocketMQ Connector SFTP 编译好的包放入Runtime加载目录。命令如下：
```
mkdir -p /usr/local/connector-plugins
cp target/rocketmq-connect-sftp-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/local/connector-plugins
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

# 核心配置，将之前编译好包的插件目录配置在此；
# Source or sink connector jar file dir,The default value is rocketmq-connect-sample
pluginPaths=/usr/local/connector-plugins
```


```
cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT

sh bin/connect-standalone.sh -c conf/connect-standalone.conf &

```

### SFTP 服务器搭建

使用 MAC OS 自带的 SFTP 服务器

[允许远程电脑访问你的 Mac](https://support.apple.com/zh-cn/guide/mac-help/mchlp1066/mac)

### 测试数据

登陆 SFTP 服务器，将具有如何内容的 souce.txt 文件放入用户目录，例如：/path/to/

```text
张三|100000202211290001|20221129001|30000.00|2022-11-28|03:00:00|7.00
李四|100000202211290002|20221129002|40000.00|2022-11-28|04:00:00|9.00
赵五|100000202211290003|20221129003|50000.00|2022-11-28|05:00:00|12.00
```

## 启动Connector

### 启动 SFTP source connector

同步 SFTP 文件：source.txt
作用：通过登陆 SFTP 服务器，解析文件并封装成通用的ConnectRecord对象，发送的RocketMQ Topic当中

```shell
curl -X POST --location "http://localhost:8082/connectors/SftpSourceConnector" --http1.1 \
    -H "Host: localhost:8082" \
    -H "Content-Type: application/json" \
    -d "{
          \"connector.class\": \"org.apache.rocketmq.connect.http.sink.SftpSourceConnector\",
          \"host\": \"127.0.0.1\",
          \"port\": 22,
          \"username\": \"wencheng\",
          \"password\": \"1617\",
          \"filePath\": \"/Users/wencheng/Documents/source.txt\",
          \"connect.topicname\": \"sftpTopic\",
          \"fieldSeparator\": \"|\",
          \"fieldSchema\": \"username|idCardNo|orderNo|orderAmount|trxDate|trxTime|profit\"
        }"
```

### 启动 SFTP sink connector

作用：通过消费Topic中的数据，通过SFTP协议写入到目标文件当中

```shell
curl -X POST --location "http://localhost:8082/connectors/SftpSinkConnector" --http1.1 \
    -H "Host: localhost:8082" \
    -H "Content-Type: application/json" \
    -d "{
          \"connector.class\": \"org.apache.rocketmq.connect.http.sink.SftpSinkConnector\",
          \"host\": \"127.0.0.1\",
          \"port\": 22,
          \"username\": \"wencheng\",
          \"password\": \"1617\",
          \"filePath\": \"/Users/wencheng/Documents/sink.txt\",
          \"connect.topicnames\": \"sftpTopic\",
          \"fieldSeparator\": \"|\",
          \"fieldSchema\": \"username|idCardNo|orderNo|orderAmount|trxDate|trxTime|profit\"
        }"
```

****