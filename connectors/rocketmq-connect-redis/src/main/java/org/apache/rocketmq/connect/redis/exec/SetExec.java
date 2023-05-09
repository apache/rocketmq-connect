/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.redis.exec;

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
