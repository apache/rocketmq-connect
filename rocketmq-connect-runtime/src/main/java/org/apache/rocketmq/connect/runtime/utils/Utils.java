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

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * common utils
 */
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Sleep for a bit
     *
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
     * Instantiate the class
     */
    public static <T> T newInstance(Class<T> c) {
        if (c == null) {
            throw new ConnectException("class cannot be null");
        }
        try {
            return c.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            throw new ConnectException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new ConnectException("Could not instantiate class " + c.getName(), e);
        }
    }

    public static <T> T newInstance(Class<T> c, Class[] parameterTypes, Object[] initargs) {
        if (c == null) {
            throw new ConnectException("class cannot be null");
        }
        try {
            return c.getDeclaredConstructor(parameterTypes).newInstance(initargs);
        } catch (NoSuchMethodException e) {
            throw new ConnectException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new ConnectException("Could not instantiate class " + c.getName(), e);
        }
    }


    public static <T> T newInstance(String klass, Class<T> base, Class[] parameterTypes, Object[] initargs) throws ClassNotFoundException {
        return Utils.newInstance(loadClass(klass, base), parameterTypes, initargs);
    }

    /**
     * new instance
     * @param klass
     * @param base
     * @return
     * @param <T>
     * @throws ClassNotFoundException
     */
    public static <T> T newInstance(String klass, Class<T> base) throws ClassNotFoundException {
        return Utils.newInstance(loadClass(klass, base));
    }


    /**
     * Create a string representation of an array joined by the given separator
     *
     * @param strs      The array of items
     * @param separator The separator
     * @return The string representation.
     */
    public static <T> String join(T[] strs, String separator) {
        return join(Arrays.asList(strs), separator);
    }

    /**
     * Create a string representation of a collection joined by the given separator
     *
     * @param collection The list of items
     * @param separator  The separator
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

    /**
     * Look up a class by name.
     */
    public static <T> Class<? extends T> loadClass(String klass, Class<T> base) throws ClassNotFoundException {
        return Class.forName(klass, true, Utils.getContextCurrentClassLoader()).asSubclass(base);
    }

    /**
     * Get current classLoader
     */
    public static ClassLoader getCurrentClassLoader() {
        return Utils.class.getClassLoader();
    }


    /**
     * get context current class loader
     *
     * @return
     */
    public static ClassLoader getContextCurrentClassLoader() {
        // use thread classloader first
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            return getCurrentClassLoader();
        }
        return cl;

    }

    /**
     * get class
     *
     * @param key
     * @return
     */
    public static Class<?> getClass(ConnectKeyValue config, final String key) {
        if (!config.containsKey(key) || StringUtils.isEmpty(config.getString(key))) {
            throw new ConnectException("");
        }
        ClassLoader contextCurrentClassLoader = Utils.getContextCurrentClassLoader();
        try {
            Class<?> klass = contextCurrentClassLoader.loadClass(config.getString(key).trim());
            return Class.forName(klass.getName(), true, contextCurrentClassLoader);
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Expected a Class instance or class name. key [" + key + "], value [" + config.getString(key) + "]");
        }
    }
}
