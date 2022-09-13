/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class MetricName implements Comparable<MetricName> {
    private String str;
    private String name;
    private String group;
    private LinkedHashMap<String, String> tags;
    private String type;

    public MetricName(String name, String group, LinkedHashMap<String, String> tags) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(group);
        Objects.requireNonNull(tags);
        this.name = name;
        this.group = group;
        this.tags = new LinkedHashMap<>(tags);
        this.str = MetricUtils.metricNameToString(this);
    }

    public MetricName(String name, String group, String type, Map<String, String> tags) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(group);
        Objects.requireNonNull(tags);
        this.name = name;
        this.group = group;
        this.type = type;
        this.tags = new LinkedHashMap<>(tags);
        this.str = MetricUtils.metricNameToString(this);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public LinkedHashMap<String, String> getTags() {
        return tags;
    }

    public void setTags(LinkedHashMap<String, String> tags) {
        this.tags = tags;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStr() {
        return MetricUtils.metricNameToString(this);
    }

    @Override
    public String toString() {
        return MetricUtils.metricNameToString(this);
    }

    @Override
    public int compareTo(MetricName o) {
        return o.toString().compareTo(this.toString());
    }

}
