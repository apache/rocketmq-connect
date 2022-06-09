# rocketmq-connect-http
* **rocketmq-connect-http** 说明
```
Be responsible for consuming messages from producer and writing data to another web service system.
```

## rocketmq-connect-http 打包
```
mvn clean install -Dmaven.test.skip=true
```

## rocketmq-connect-http 启动

* **http-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-http-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.http.sink.HttpSinkConnector",
"urlPattern":"${urlPattern}","method":"${method}","queryStringParameters":"${queryStringParameters}","headerParameters":"${headerParameters}","bodys":"${bodys}","authType":"${authType}","basicUser":"${basicUser}","basicPassword":"${basicPassword}",
"oauth2Endpoint":"${oauth2Endpoint}","oauth2ClientId":"${oauth2ClientId}","oauth2ClientSecret":"${oauth2ClientSecret}","oauth2HttpMethod":"${oauth2HttpMethod}","proxyType":"${proxyType}","proxyHost":"${proxyHost}","proxyPort":"${proxyPort}","proxyUser":"${proxyUser}",
"proxyPort":"${proxyPort}","proxyPort":"${proxyPort}","proxyUser":"${proxyUser}","proxyPassword":"${proxyPassword}","apiKeyName":"${apiKeyName}","apiKeyValue":"${apiKeyValue}","timeout":"${timeout}"}
```

例子
```
http://localhost:8081/connectors/httpConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.http.sink.HttpSinkConnector","urlPattern":"http://127.0.0.1","method":"POST","queryStringParameters":"","headerParameters":"","bodys":"{"id" : "234"}","authType":"BASIC_AUTH","basicUser":"","basicPassword":"",
"oauth2Endpoint":"","oauth2ClientId":"","oauth2ClientSecret":"","oauth2HttpMethod":"","proxyType":"","proxyHost":"","proxyPort":"","proxyUser":"",
"proxyPort":"","proxyPort":"","proxyUser":"","proxyPassword":"","apiKeyName":"","apiKeyValue":"","timeout":"6000"}
```

* **api-destination-sink-connector** 启动

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-api-destination-sink-connector-name}
?config={"source-rocketmq":"${runtime-ip}:${runtime-port}","source-cluster":"${broker-cluster}","connector-class":"org.apache.rocketmq.connect.http.sink.ApiDestinationSinkConnector",
"apiDestinationName":"${apiDestinationName}","endpoint":"${endpoint}","pushCertPrivateKey":"${pushCertPrivateKey}","pushCertSignMethod":"${pushCertSignMethod}","pushCertSignVersion":"${pushCertSignVersion}","pushCertPublicKeyUrl":"${pushCertPublicKeyUrl}"}
```

例子
```
http://localhost:8081/connectors/apiDestinationConnectorSink?config={"source-rocketmq":"localhost:9876","source-cluster":"DefaultCluster",
"connector-class":"org.apache.rocketmq.connect.http.sink.ApiDestinationSinkConnector","apiDestinationName":"xxxx","endpoint":"http://127.0.0.1","pushCertPrivateKey":"","pushCertSignMethod":"","pushCertSignVersion":"","pushCertPublicKeyUrl":""}
```

>**注：** `rocketmq-http-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-http 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-http-connector-name}/stop
```

## rocketmq-connect-http 参数说明
* **http-sink-connector 参数说明**

| KEY                   |  TYPE   | Must be filled | Description    | Example          
|-----------------------|---------|----------------|----------------|------------------|
| urlPattern            | String  | YES            | sink端 域名地址     | http://127.0.0.1 |
| method                | String  | YES            | 请求类型           | POST、GET         |
| queryStringParameters | String  | NO             | 请求参数           | xxxx             |
| headerParameters      | String  | NO             | 请求头            | xxxx             |
| bodys                 | String  | YES            | 请求体            | xxxx             |
| authType              | String  | YES            | 权限类型           | BASIC_AUTH、OAUTH_AUTH、API_KEY_AUTH   |
| basicUser             | String  | NO            | 用户名            | xxxx             |
| basicPassword         | String  | NO            | 密码             | xxxx             |
| oauth2Endpoint        | String  | NO            | OAuth获取token地址 | http://127.0.0.1 |
| oauth2ClientId        | String  | NO            | clientId       | xxxx             |
| oauth2ClientSecret    | String  | NO            | client secret  | xxxx             |
| oauth2HttpMethod      | String  | NO            | oauth的请求类型     | xxxx             |
| proxyType             | String  | NO            | 代理类型           | xxxx             |
| proxyHost             | String  | NO            | 代理地址           | xxxx             |
| proxyPort             | String  | NO            | 代理端口           | xxxx             |
| proxyUser             | String  | NO            | 代理的访问的用户名      | xxxx             |
| proxyPassword         | String  | NO            | 代理访问的密码        | xxxx             |
| apiKeyName            | String  | NO            | auth api key   | xxxx             |
| apiKeyValue           | String  | NO            | auth api value | xxxx             |
| timeout               | String  | NO            | 超时时间           | xxxx             |
| connect-topicname     | String  | YES             | sink需要处理数据消息topic                     | xxxx |

* **api-destination-sink-connector 参数说明**

| KEY                  |  TYPE   | Must be filled | Description          | Example          
|----------------------|---------|----------------|----------------------|------------------|
| apiDestinationName   | String  | YES            | api destination name |                  |
| endpoint             | String  | YES            | 请求地址                 | http://127.0.0.1 |
| pushCertPrivateKey   | String  | NO             |                  | xxxx             |
| pushCertSignMethod   | String  | NO             |                   | xxxx             |
| pushCertSignVersion  | String  | NO             |                   | xxxx             |
| pushCertPublicKeyUrl | String  | NO             |                  | xxxx             |
| connect-topicname    | String  | YES             | sink需要处理数据消息topic                     | xxxx |
