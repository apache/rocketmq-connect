package org.apache.rocketmq.connect.redis.common;

import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil {
    
    public static JedisPool getJedisPool(Config config) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(50);
        jedisPoolConfig.setMaxWaitMillis(3000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        String pwd = null;
        if (StringUtils.isNotBlank(config.getRedisPassword())) {
            pwd = config.getRedisPassword();
        }
        
        return new JedisPool(jedisPoolConfig,
                config.getRedisAddr(),
                config.getRedisPort(),
                config.getTimeout(),
                pwd);
    }
}
