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

package org.apache.rocketmq.redis.test.connector;

import java.util.List;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.redis.connector.RedisSourceConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSourceConnectorTest {
    private KeyValue keyValue;

    @Before
    public void initKeyValue(){
        this.keyValue = new DefaultKeyValue();
        this.keyValue.put("redisAddr", "127.0.0.1");
        this.keyValue.put("redisPort", "6379");
        this.keyValue.put("redisPassword", "");
    }
    @Test
    public void testConnector(){
        RedisSourceConnector connector = new RedisSourceConnector();
        connector.verifyAndSetConfig(this.keyValue);
        Class cl = connector.taskClass();
        Assert.assertNotNull(cl);
        List<KeyValue> keyValues = connector.taskConfigs();
        Assert.assertNotNull(keyValues);
        connector.start();
        connector.pause();
        connector.resume();
        connector.stop();
    }
}
