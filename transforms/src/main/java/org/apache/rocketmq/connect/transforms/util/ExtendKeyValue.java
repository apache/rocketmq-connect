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

package org.apache.rocketmq.connect.transforms.util;

import io.openmessaging.KeyValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * extend key value
 */
public class ExtendKeyValue implements KeyValue {
    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");

    private KeyValue config;

    public ExtendKeyValue(KeyValue config) {
        this.config = config;
    }

    @Override
    public KeyValue put(String s, int i) {
        return config.put(s, i);
    }

    @Override
    public KeyValue put(String s, long l) {
        return config.put(s, l);
    }

    @Override
    public KeyValue put(String s, double v) {
        return config.put(s, v);
    }

    @Override
    public KeyValue put(String s, String s1) {
        return config.put(s, s1);
    }

    @Override
    public int getInt(String s) {
        return config.getInt(s);
    }

    @Override
    public int getInt(String s, int i) {
        return config.getInt(s, i);
    }

    @Override
    public long getLong(String s) {
        return config.getLong(s);
    }

    @Override
    public long getLong(String s, long l) {
        return config.getLong(s, l);
    }

    @Override
    public double getDouble(String s) {
        return config.getDouble(s);
    }

    @Override
    public double getDouble(String s, double v) {
        return config.getDouble(s, v);
    }

    @Override
    public String getString(String s) {
        return config.getString(s);
    }

    @Override
    public String getString(String s, String s1) {
        return config.getString(s, s1);
    }

    @Override
    public Set<String> keySet() {
        return config.keySet();
    }

    @Override
    public boolean containsKey(String s) {
        return config.containsKey(s);
    }

    /**
     * get list
     *
     * @param s
     * @return
     */
    public List getList(String s) {
        if (!this.config.containsKey(s)) {
            return new ArrayList();
        }
        String config = this.config.getString(s).trim();
        return Arrays.asList(COMMA_WITH_WHITESPACE.split(config, -1));
    }

    /**
     * get list by class
     *
     * @param s
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<T> getList(String s, Class<T> clazz) {
        List configs = getList(s);
        List<T> castConfigs = new ArrayList<>();
        configs.forEach(config -> {
            castConfigs.add(clazz.cast(config));
        });
        return castConfigs;
    }
}
