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
 *
 */

package org.apache.rocketmq.replicator.stats;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
public class TpsLimiter {
    private static volatile ConcurrentMap<String, Invoke> invokeCache = new ConcurrentHashMap<>(64);
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    static class Invoke {
        AtomicInteger secondPv = new AtomicInteger();
        AtomicLong second = new AtomicLong(System.currentTimeMillis() / 1000L);
    }
    public static void addPv(String key, long totalPv) {
        if (totalPv <= 0) {
            return;
        }
        Invoke invoke = invokeCache.get(key);
        if (invoke == null) {
            try {
                lock.readLock().lock();
                invokeCache.putIfAbsent(key, new Invoke());
                invoke = invokeCache.get(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        long now = nowSecond();
        AtomicLong oldSecond = invoke.second;
        if (oldSecond.get() == now) {
            invoke.secondPv.addAndGet((int)totalPv);
        } else {
            if (oldSecond.compareAndSet(oldSecond.get(), now)) {
                invoke.secondPv.set(1);
            } else {
                invoke.secondPv.addAndGet((int)totalPv);
            }
        }
    }

    private static long nowSecond() {
        return System.currentTimeMillis() / 1000L;
    }

    public static boolean isOverFlow(String key, int tps) {
        return nowTps(key) >= tps;
    }

    public static int nowTps(String key) {
        Invoke invoke = invokeCache.get(key);
        if (invoke == null) {
            return 0;
        }
        AtomicLong oldSecond = invoke.second;
        if (oldSecond.get() == nowSecond()) {
            return invoke.secondPv.get();
        }
        return 0;
    }
}
