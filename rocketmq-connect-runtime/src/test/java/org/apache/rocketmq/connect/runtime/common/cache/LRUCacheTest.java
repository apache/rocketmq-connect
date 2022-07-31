package org.apache.rocketmq.connect.runtime.common.cache;

import org.apache.commons.lang3.StringUtils;
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
        lruCache.put("key1", "value1");
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
