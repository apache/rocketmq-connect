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

package org.apache.rocketmq.connect.runtime.common;

import io.openmessaging.KeyValue;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default Implements of {@link KeyValue} for runtime, which can be parsed by fastJson.
 */
public class ConnectKeyValue implements KeyValue, Serializable, Cloneable {

    /**
     * epoch for update
     */
    private long epoch;

    /**
     * target state
     */
    private TargetState targetState;

    /**
     * All data are reserved in this map.
     */
    private Map<String, String> properties;

    public ConnectKeyValue() {
        properties = new ConcurrentHashMap<>();
    }

    @Override
    public KeyValue put(String key, int value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public int getInt(String key) {
        if (!properties.containsKey(key))
            return 0;
        return Integer.valueOf(properties.get(key));
    }

    @Override
    public int getInt(final String key, final int defaultValue) {
        return properties.containsKey(key) ? getInt(key) : defaultValue;
    }

    @Override
    public long getLong(String key) {
        if (!properties.containsKey(key))
            return 0;
        return Long.valueOf(properties.get(key));
    }

    @Override
    public long getLong(final String key, final long defaultValue) {
        return properties.containsKey(key) ? getLong(key) : defaultValue;
    }

    @Override
    public double getDouble(String key) {
        if (!properties.containsKey(key))
            return 0;
        return Double.valueOf(properties.get(key));
    }

    @Override
    public double getDouble(final String key, final double defaultValue) {
        return properties.containsKey(key) ? getDouble(key) : defaultValue;
    }

    @Override
    public String getString(String key) {
        return properties.get(key);
    }

    @Override
    public String getString(final String key, final String defaultValue) {
        return properties.containsKey(key) ? getString(key) : defaultValue;
    }

    @Override
    public Set<String> keySet() {
        return properties.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Gets all original settings with the given prefix.
     */
    public Map<String, String> originalsWithPrefix(String prefix) {
        return originalsWithPrefix(prefix, true);
    }

    /**
     * Gets all original settings with the given prefix.
     *
     * @param prefix the prefix to use as a filter
     * @param strip  strip the prefix before adding to the output if set true
     * @return a Map containing the settings with the prefix
     */
    public Map<String, String> originalsWithPrefix(String prefix, boolean strip) {
        Map<String, String> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                if (strip) {
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    public TargetState getTargetState() {
        return targetState;
    }

    public void setTargetState(TargetState targetState) {
        this.targetState = targetState;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ConnectKeyValue nextGeneration() {
        this.setEpoch(System.currentTimeMillis());
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj.getClass() == this.getClass()) {
            ConnectKeyValue keyValue = (ConnectKeyValue) obj;
            return this.properties.equals(keyValue.getProperties());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "ConnectKeyValue{" +
                "properties=" + properties +
                '}';
    }

}
