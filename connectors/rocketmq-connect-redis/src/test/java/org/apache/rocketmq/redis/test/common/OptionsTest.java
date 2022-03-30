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

package org.apache.rocketmq.redis.test.common;

import org.apache.rocketmq.connect.redis.common.Options;
import org.junit.Assert;
import org.junit.Test;


public class OptionsTest {

    @Test
    public void test(){
        Options options1 = Options.valueOf("TEST");
        Assert.assertNull(options1);

        Options options2 = Options.REDIS_PARTITION;
        Assert.assertEquals(options2.name(), "DEFAULT_PARTITION");

        Assert.assertNotNull(options2.toString());

        Options options3 = Options.valueOf("DEFAULT_PARTITION");
        Assert.assertTrue(options3.equals(options2));
    }

    @Test
    public void testNull(){
        Exception ex = null;
        try {
            Options.valueOf("");
        }catch (Exception e){
            ex = e;
        }
        Assert.assertNotNull(ex);
    }
}
