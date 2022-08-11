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

package org.apache.rocketmq.connect.runtime.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 *  common utils
 */
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    /**
     * Sleep for a bit
     * @param ms The duration of the sleep
     */
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // this is okay, we just wake up early
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes {@code closeable} and if an exception is thrown, it is logged at the WARN level.
     */
    public static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                log.warn("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }

    /**
     * Create a string representation of an array joined by the given separator
     * @param strs The array of items
     * @param separator The separator
     * @return The string representation.
     */
    public static <T> String join(T[] strs, String separator) {
        return join(Arrays.asList(strs), separator);
    }

    /**
     * Create a string representation of a collection joined by the given separator
     * @param collection The list of items
     * @param separator The separator
     * @return The string representation.
     */
    public static <T> String join(Collection<T> collection, String separator) {
        Objects.requireNonNull(collection);
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = collection.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
}
