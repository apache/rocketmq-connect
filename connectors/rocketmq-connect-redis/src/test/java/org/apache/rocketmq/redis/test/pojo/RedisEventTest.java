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

package org.apache.rocketmq.redis.test.pojo;

import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import org.apache.rocketmq.connect.redis.pojo.RedisEvent;
import org.junit.Assert;
import org.junit.Test;

public class RedisEventTest {
    @Test
    public void test(){
        RedisEvent redisEvent = new RedisEvent();
        redisEvent.setEvent(getKeyValuePair());
        redisEvent.setReplOffset(3926872L);
        redisEvent.setReplId("c18cece63c7b16851a6f387f52dbbb9eee07e46f");
        redisEvent.setStreamDB(0);

        Assert.assertEquals("c18cece63c7b16851a6f387f52dbbb9eee07e46f", redisEvent.getReplId());
        Assert.assertTrue(3926872L == redisEvent.getReplOffset());
        Assert.assertTrue(0 == redisEvent.getStreamDB());
        Assert.assertNotNull(redisEvent.getEvent());
        Assert.assertEquals(KeyStringValueString.class, redisEvent.getEvent().getClass());
        Assert.assertEquals("key", new String(((KeyStringValueString)redisEvent.getEvent()).getKey()));
        Assert.assertEquals("value", new String(((KeyStringValueString)redisEvent.getEvent()).getValue()));
    }

    private KeyValuePair getKeyValuePair(){
        KeyValuePair pair = new KeyStringValueString();
        pair.setKey("key".getBytes());
        pair.setValue("value".getBytes());
        return pair;
    }
}
