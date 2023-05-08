package org.apache.rocketmq.connect.redis.builder;

import org.apache.rocketmq.connect.redis.common.RedisConstants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Map;
import java.util.Objects;

public class SetExec extends AbstractCommandExec {
    
    @Override
    public String exec(Jedis jedis, String key, final String value, final Map<String, Object> params) {
        SetParams setParams = new SetParams();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (Objects.equals(RedisConstants.NX, entry.getKey())) {
                setParams.nx();
            } else if (Objects.equals(RedisConstants.XX, entry.getKey())) {
                setParams.xx();
            } else if (Objects.equals(RedisConstants.KEEPTTL, entry.getKey())) {
                setParams.keepttl();
            } else if (Objects.equals(RedisConstants.EX, entry.getKey())) {
                setParams.ex(Long.parseLong(entry.getValue().toString()));
            } else if (Objects.equals(RedisConstants.PX, entry.getKey())) {
                setParams.px(Long.parseLong(entry.getValue().toString()));
            }
        }
        return setParams.getByteParams().length == 0 ? jedis.set(key, value) : jedis.set(key, value, setParams);
    }
}
