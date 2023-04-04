# rocketmq-connect-sftp

Plugin for Rocketmq Connect. Tansfer file based on SFTP.

# How to use

* start rocketmq nameserver

```shell
cd ${ROCKETMQ_HOME}
nohup ./bin/mqnamesrv &
```

* start rocketmq broker

```shell
nohup ./bin/mqbroker -n localhost:9876 &
```

* build plugin

```shell
cd connectors/rocketmq-connect-sftp
mvn clean install -Dmaven.test.skip=true
```

* create config file path/to/connect-standalone.conf same as distribution/conf/connect-standalone.conf
* modify pluginPaths=path/to/rocketmq-connect-sftp-0.0.1-SNAPSHOT-jar-with-dependencies
* start org.apache.rocketmq.connect.runtime.StandaloneConnectStartup

```shell
cd rocketmq-connect-runtime
mvn clean install -Dmaven.test.skip=true
```

* start source connector

```http request
POST /connectors/SftpSourceConnector HTTP/1.1
Host: localhost:8082
Content-Type: application/json

{
  "connector.class": "org.apache.rocketmq.connect.http.sink.SftpSourceConnector",
  "host": "127.0.0.1",
  "port": 22,
  "username": "wencheng",
  "password": "",
  "filePath": "/Users/wencheng/Documents/source.txt",
  "connect.topicname": "sftpTopic",
  "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
	"fieldSeparator": "\\|",
  "fieldSchema": "username|idCardNo|orderNo|orderAmount|trxDate|trxTime|profit"
}
```

`warning:` make sure exist a file named "source.txt" on the sftp server.

* start sink connector

```http request
POST /connectors/SftpSinkConnector HTTP/1.1
Host: localhost:8082
Content-Type: application/json

{
  "connector.class": "org.apache.rocketmq.connect.http.sink.SftpSinkConnector",
  "host": "127.0.0.1",
  "port": 22,
  "username": "wencheng",
  "password": "",
  "filePath": "/Users/wencheng/Documents/sink.txt",
  "connect.topicnames": "sftpTopic"
}
```

## What we expected to see

The file named sink.txt will be created, and the content of the "source.txt" will appears in this file.

## Appendix: Connector Configuration

### sftp-source-connector configuration

| KEY               | TYPE   | REQUIRED | DESCRIPTION                                            | EXAMPLE             |
| ----------------- | ------ | -------- | ------------------------------------------------------ | ------------------- |
| host              | String | Y        | SFTP host                                              | localhost           |
| port              | int    | Y        | SFTP port                                              | 22                  |
| username          | String | Y        | SFTP username                                          | wencheng            |
| password          | String | Y        | SFTP password                                          |                     |
| filePath          | String | Y        | The name of the file which will be transferred         | /path/to/source.txt |
| fieldSchema       | String | Y        | the data schema of each line                           |                     |
| fieldSeparator    | String | Y        | Symbol that separates each field                       |                     |
| connect.topicname | String | Y        | The Message Queue topic which the data will be send to |                     |

### sftp-sink-connector configuration

| KEY                | TYPE   | REQUIRED | DESCRIPTION                                                | EXAMPLE           |
| ------------------ | ------ | -------- | ---------------------------------------------------------- | ----------------- |
| host               | String | Y        | SFTP host                                                  | localhost         |
| port               | int    | Y        | SFTP port                                                  | 22                |
| username           | String | Y        | SFTP username                                              | wencheng          |
| password           | String | Y        | SFTP password                                              |                   |
| filePath           | String | Y        | The name of the file which will be transferred             | /path/to/sink.txt |
| connect.topicnames | String | Y        | The Message Queue topic which the data will be pulled from |                   |
| fieldSchema        | String | Y        | the data schema of each line                               |                   |
| fieldSeparator     | String | Y        | Symbol that separates each field                           |                   |





