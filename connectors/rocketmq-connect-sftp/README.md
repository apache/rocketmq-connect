# rocketmq-connect-sftp

Plugin for Rocketmq Connect. Tansfer file based on SFTP.

## How to use

```shell
nohup ./bin/mqnamesrv &
nohup ./bin/mqbroker -n localhost:9876 &
mvn clean install -Dmaven.test.skip=true
```

## How to start source connector

```http request
POST /connectors/SftpSourceConnector HTTP/1.1
Host: localhost:8082
Content-Type: application/json

{
  "connector.class": "org.apache.rocketmq.connect.http.sink.SftpSourceConnector",
  "host": "127.0.0.1",
  "port": 22,
  "filename": "NGPProcessProcessor.db",
  "username": "wencheng",
  "password": "1617",
  "path": "Documents",
  "connect.topicnames": "3",
  "topic": "sftpTopic"
}
```

## How to start sink connector

```http request
POST /connectors/SftpSourceConnector HTTP/1.1
Host: localhost:8082
Content-Type: application/json

{
  "connector.class": "org.apache.rocketmq.connect.http.sink.SftpSourceConnector",
  "host": "sftp://jack@localhost:22/Documents",
  "port": 22,
  "filename": "dump.txt",
  "username": "jack",
  "password": "",
  "topic": ""
}
```

## Connector Configuration

### sftp-source-connector configuration

| KEY      | TYPE   | REQUIRED | DESCRIPTION                                            | EXAMPLE               |
|----------|--------|----------|--------------------------------------------------------|-----------------------|
| host     | String | Y        | SFTP host                                              | localhost             |
| port     | int    | Y        | SFTP port                                              | 22                    |
| username | String | Y        | SFTP username                                          | jack                  |
| password | String | Y        | SFTP password                                          |                       |
| path     | String | Y        | SFTP path                                              | /Users/jack/Documents |
| filename | String | Y        | The name of the file which will be transferred         | dump.txt              |
| topic    | String | Y        | The Message Queue topic which the data will be send to |                       |

### sftp-sink-connector configuration

| KEY      | TYPE   | REQUIRED | DESCRIPTION                                            | EXAMPLE               |
|----------|--------|----------|--------------------------------------------------------|-----------------------|
| host     | String | Y        | SFTP host                                              | localhost             |
| port     | int    | Y        | SFTP port                                              | 22                    |
| username | String | Y        | SFTP username                                          | jack                  |
| password | String | Y        | SFTP password                                          |                       |
| path     | String | Y        | SFTP path                                              | /Users/jack/Documents |
| filename | String | Y        | The name of the file which will be transferred         | dump.txt              |
| topic    | String | Y        | The Message Queue topic which the data will be send to |                       |





