#connect docker start
##准备

###环境
1. Linux/Unix/Mac
2. 64bit JDK 1.8+;
3. Maven 3.2.x或以上版本;
4. 安装docker

###启动RocketMQ

RocketMQ镜像下载

https://hub.docker.com/r/apache/rocketmq/tags

RocketMQ镜像启动

RocketMQ镜像 quick start

###构建Connector镜像
```
sh build_image.sh
```

##启动Connector

```
docker run  --name rmqconnect --link rmqnamesrv:namesrv -e "NAMESRV_ADDR=namesrv:9876" -p 8082:8082 apache/rocketmqconnect:0.0.1-SNAPSHOT sh /home/connect/mq-connect/bin/connect-distributed.sh -c /home/connect/mq-connect/conf/connect-distributed.conf
```