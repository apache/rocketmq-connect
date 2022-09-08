package org.apache.rocketmq.connect.runtime.metrics;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class MetricName {
    private final String name;
    private final String group;
    private final Map<String, String> tags;
    private final String str;

    public MetricName(String name, String group, Map<String, String> tags) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(group);
        Objects.requireNonNull(tags);
        this.name = name;
        this.group = group;
        this.tags = Collections.unmodifiableMap(new LinkedHashMap<>(tags));
        this.str = MetricUtils.metricNameToString(this);
    }

    /**
     * Get the group name.
     *
     * @return the group name; never null
     */
    public String group() {
        return group;
    }

    /**
     * Get the name.
     *
     * @return the group name; never null
     */
    public String name() {
        return name;
    }

    /**
     * Get the immutable map of tag names and values.
     *
     * @return the tags; never null
     */
    public Map<String, String> tags() {
        return tags;
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.tags);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj instanceof MetricName) {
            MetricName that = (MetricName) obj;
            return this.name.equals(that.name) && this.tags.equals(that.tags);
        }
        return false;
    }

    @Override
    public String toString() {
        return str;
    }
}
