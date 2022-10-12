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
package org.apache.rocketmq.connect.runtime.metrics;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class MetricNameTemplate {
    private final String name;
    private final String group;
    private LinkedHashSet<String> tags;

    public MetricNameTemplate(String name, String group, Set<String> tagsNames) {
        this.name = Objects.requireNonNull(name);
        this.group = Objects.requireNonNull(group);
        this.tags = new LinkedHashSet<>(Objects.requireNonNull(tagsNames));
    }

    public String getName() {
        return name;
    }

    public String getGroup() {
        return group;
    }

    public LinkedHashSet<String> getTags() {
        return tags;
    }

    public void setTags(LinkedHashSet<String> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricNameTemplate that = (MetricNameTemplate) o;
        return Objects.equals(name, that.name) && Objects.equals(group, that.group) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, group, tags);
    }

    @Override
    public String toString() {
        return "MetricNameTemplate{" +
                "name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", tags=" + tags +
                '}';
    }
}
