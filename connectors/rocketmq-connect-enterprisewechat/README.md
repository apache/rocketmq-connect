# rocketmq-connect-enterprisewechat

##### EnterpriseWechatSinkConnector 完全限定名

org.apache.rocketmq.connect.enterprisewechat.sink.EnterpriseWechatSinkConnector

- **enterprisewechat-sink-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/enterpriseWechatSinkConnector
{
    "connector.class":"org.apache.rocketmq.connect.enterprisewechat.sink.EnterpriseWechatSinkConnector",
    "max.tasks":"3",
    "connect.topicname":"targetTopic",
    "webHook": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxx"
}
```