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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * lru cache
 *
 * @param <K>
 * @param <V>
 */
public class LRUCache<K, V> implements Cache<K, V> {

    private final Map<K, V> cache;

    public LRUCache(final int maxSize) {
        cache = Collections.synchronizedMap(new LinkedHashMap<K, V>(300, 1.1F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return this.size() > maxSize;
            }
        });
    }

    /**
     * @param key
     * @return
     */
    @Override
    public V get(K key) {
        return cache.get(key);
    }

    /**
     * put a data to cache
     *
     * @param key
     * @param value
     */
    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    /**
     * remove a data to cache
     *
     * @param key
     * @return
     */
    @Override
    public boolean remove(K key) {
        return cache.remove(key) != null;
    }

    /**
     * cache size
     *
     * @return
     */
    @Override
    public long size() {
        return cache.size();
    }
}
