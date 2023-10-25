# rocketmq-connect-enterprisewechat

## 快速开始

### 1. 构建 rocketmq-connect-enterprisewechat

```bash
cd connectors/rocketmq-connect-enterprisewechat

mvn -DskipTests clean package
```

打包好的文件在 `connectors/rocketmq-connect-enterprisewechat/target` 目录下

### 2. 修改配置

修改 `distribution/conf` 目录下对应的配置文件。如对于 standalone 的启动方式，修改 `connect-standalone.conf`
文件中的 `namesrvAddr` 和 `pluginPaths` 等字段：

```properties
namesrvAddr=localhost:9876
pluginPaths=connectors/rocketmq-connect-enterprisewechat/target/rocketmq-connect-enterprisewechat-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

### 3. 启动 Worker

参见 [README](https://github.com/apache/rocketmq-connect/blob/master/README.md#3%E8%BF%90%E8%A1%8Cworker)

### 4. 启动 Sink Connector

```http request
POST  http://127.0.0.1:8082/connectors/enterpriseWechatSinkConnector
Content-Type: application/json

{
  "connector.class": "org.apache.rocketmq.connect.enterprisewechat.sink.EnterpriseWechatSinkConnector",
  "connect.topicnames": "testTopic",
  "webHook": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxx"
}
```

### 5. 停止 Sink Connector

```http request
GET http://${runtime-ip}:${runtime-port}/connectors/enterpriseWechatSinkConnector/stop
```

## 参数说明

| KEY                | TYPE   | Must be filled | Description     | Example                                                   |
|--------------------|--------|----------------|-----------------|-----------------------------------------------------------|
| webHook            | String | YES            | 机器人的webhook地址   | https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx |
| connect.topicnames | String | YES            | sink消费消息的topics | testTopic                                                 |

## Record 数据格式说明
Record 应保存 JSON 字符串如：

```json
{
  "msgtype": "text",
  "text": {
    "content": "Hello World!"
  }
}
```

详情可参见[官方文档](https://developer.work.weixin.qq.com/document/path/99110#%E5%A6%82%E4%BD%95%E4%BD%BF%E7%94%A8%E7%BE%A4%E6%9C%BA%E5%99%A8%E4%BA%BA)。