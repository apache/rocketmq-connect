# RocketMQ-connect-redis
##### RedisSourceConnector fully-qualified name
org.apache.rocketmq.connect.redis.connector.RedisSourceConnector


* **redis-source-connector** start

```
POST  http://${runtime-ip}:${runtime-port}/connectors/redisSourceConnector
{
    "connector.class":"org.apache.rocketmq.connect.redis.connector.RedisSourceConnector",
    "max.tasks":"3",
    "redisAddr":"localhost",
    "redisPort":"6379",
    "connect.topicname":"targetTopic",
    "value.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter":"org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

##### parameter configuration

parameter | effect | required |default
---|--- |--- | ---
redisAddr | The address of the Redis. | yes | null
redisPort | The port fo the Redis address. | yes | null
redisPassword | The password to use when connecting to Redis. | yes | null
timeout | The waiting time before connect to Redis success. | no | 3000
syncMod | The mod for how to get data from redis. | no | CUSTOM_OFFSET
offset | The position of Redis data. | no | -1 
replId | The master replyId of Redis, which can get it with command "info" by redis-cli from Redis. | no  | null 
commands | The Redis commands you want to sync, they are useful only for increment Redis data, multiple commands are separated by commas. | 否 | *
eventCommitRetryTimes | The retry time when receive Redis change event, failed to commit to RedisEventProcessor. | no | 5
eventCommitRetryInterval | The time when receive Redis change, failed to commit to RedisEventProcessor and retry commit. | no | 100
