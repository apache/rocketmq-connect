# rocketmq-connect-http
* **rocketmq-connect-http** 说明
```
Be responsible for consuming messages from producer and writing data to another web service system.
```

## rocketmq-connect-http使用方法

1. 进入想要使用的connectors目录下（以rocketmq-connect-http目录为例），使用以下指令将插件进行打包
   ```shell
   mvn clean package -Dmaven.test.skip=true
   ```
2. 打包好的插件以jar包的模式出现在`rocketmq-connect-http/target/`目录下

3. 在`distribution/conf`目录下找的对应的配置文件进行更新，对于standalone的启动方式，更新`connect-standalone.conf`文件中的`pluginPaths`变量

   ```
   pluginPaths=(you plugin path)
   ```

   相应的，使用distributed启动方式，则更新`connect-distributed.conf`中的变量
4. 创建并启动对应的`SourceConnector`以及`SinkConnector`


## rocketmq-connect-http 启动

* **http-sink-connector** 启动

```
POST  http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-jdbc-source-connector-name}
{
    "connector.class":"org.apache.rocketmq.connect.http.HttpSinkTask",
    "url":"${url}",
    "method":"${method}",
    "connect.topicnames":"${connect.topicnames}"
}
```

例子
```
http://localhost:8081/connectors/httpSinkConnector?config={"connector-class":"org.apache.rocketmq.connect.http.HttpSinkTask","connect-topicname" : "http-topic","url":"192.168.1.2"}
```
```在请求中定义http header、query、body参数
http://127.0.0.1:8082/connectors/httpSinkConnector?config={"connector.class":"org.apache.rocketmq.connect.http.HttpSinkConnector","url":"http://localhost:8080/api","timeout":"6000","connect.topicnames":"fileTopic","headerParameters":"{\"header1k\":\"header1v\"}","method":"POST","queryParameters":"{\"queryk1\":\"queryv1\"}"}
```

更多参数见[rocketmq-connect-http 参数说明](#rocketmq-connect-http-参数说明)

>**注：** `rocketmq-http-connect` 的启动依赖于`rocketmq-connect-runtime`项目的启动，需将打好的所有`jar`包放置到`runtime`项目中`pluginPaths`配置的路径后再执行上面的启动请求,该值配置在`runtime`项目下的`connect.conf`文件中

## rocketmq-connect-http 停止

```
http://${runtime-ip}:${runtime-port}/connectors/${rocketmq-http-connector-name}/stop
```

## rocketmq-connect-http 参数说明
* **http-sink-connector 参数说明**

| KEY                   | TYPE   | Must be filled | Description                                    | Example                   |
|-----------------------|--------|----------------|------------------------------------------------|---------------------------|
| connect-topicname     | String | YES            | sink需要处理数据消息topic                              | fileTopic                 |
| url                   | String | YES            | 目标端url地址                                       | http://localhost:8080/api |
| method                | String | YES            | http请求方法                                       | POST                      |
| body                  | String | No             | http请求body字段，不填时默认使用事件的Data字段                  | POST                      |
| headerParameters      | String | NO             | http请求header map动态参数Json字符串                    | {"key1":"value1"}         |
| fixedHeaderParameters | String | NO             | http请求header map静态参数Json字符串                    | {"key1":"value1"}         |
| queryParameters       | String | NO             | http请求query map动态参数Json字符串                     | {"key1":"value1"}         |
| fixedQueryParameters  | String | NO             | http请求query map静态参数Json字符串                     | {"key1":"value1"}         |
| socks5UserName        | String | NO             | sock5代理用户名                                     | *****                     |
| socks5Password        | String | NO             | sock5代理密码                                      | *****                     |
| socks5Endpoint        | String | NO             | sock5代理地址                                      | http://localhost:7000     |
| timeout               | String | NO             | http请求超时时间(毫秒)                                 | 3000                      |
| concurrency           | String | NO             | http请求并发数                                      | 1                         |
| authType              | String | NO             | 认证方式 (BASIC_AUTH/OAUTH_AUTH/API_KEY_AUTH/NONE) | BASIC_AUTH                |
| basicUsername         | String | NO             | basic auth username                            | *****                     |
| basicPassword         | String | NO             | basic auth password                            | *****                     |
| apiKeyUsername        | String | NO             | api key auth username                          | *****                     |
| apiKeyPassword        | String | NO             | api key auth password                          | *****                     |
| oAuthEndpoint         | String | NO             | oauth 地址                                       | http://localhost:7000     |
| oAuthHttpMethod       | String | NO             | oauth http请求方法                                 | GET                       |
| oAuthClientId         | String | NO             | oauth client id                                | xxxx                      |
| oAuthClientSecret     | String | NO             | oauth client secret                            | xxxx                      |
| oAuthHeaderParameters | String | NO             | oauth header map参数Json字符串                      | {"key1":"value1"}         |
| oAuthQueryParameters  | String | NO             | oauth query map参数Json字符串                       | {"key1":"value1"}         |
| oAuthBody             | String | NO             | oauth body参数                                   | bodyData                  |
| token                 | String | NO             | http请求token，如果非空，会添加到http请求的header中，key为token  | xxxx                      |
