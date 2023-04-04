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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.common.cache;

import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LRUCacheTest {

    private LRUCache<String, String> lruCache;

    @Before
    public void before() {
        lruCache = new LRUCache<>(5);
    }

    @After
    public void after() {

    }

    @Test
    public void getTest() {
        // not exist
        final String value1 = lruCache.get("key1");
        assert StringUtils.isEmpty(value1);

        // put then get
        lruCache.put("key2", "value2");
        final String value2 = lruCache.get("key2");
        assert StringUtils.isNotEmpty(value2);
    }

    @Test
    public void putTest() {
        Assertions.assertThatCode(() -> lruCache.put("key1", "value1")).doesNotThrowAnyException();
    }

    @Test
    public void removeTest() {
        lruCache.put("key1", "value1");
        final boolean result1 = lruCache.remove("key1");
        assert result1 == true;
        final boolean result2 = lruCache.remove("key2");
        assert result2 == false;
    }

    @Test
    public void sizeTest() {
        final long size1 = lruCache.size();
        assert size1 == 0;
        lruCache.put("key1", "value1");
        final long size2 = lruCache.size();
        assert size2 == 1;
        lruCache.put("key2", "value2");
        final long size3 = lruCache.size();
        assert size3 == 2;
    }
}
