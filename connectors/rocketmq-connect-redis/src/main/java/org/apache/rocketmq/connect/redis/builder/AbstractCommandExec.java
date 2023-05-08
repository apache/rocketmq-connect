package org.apache.rocketmq.connect.redis.builder;

import redis.clients.jedis.Jedis;

import java.util.Map;

public abstract class AbstractCommandExec {
    
    public abstract String exec(Jedis jedis, String key, String value, Map<String, Object> params);
    
}
