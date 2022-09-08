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

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * metric utils
 */
public class MetricUtils {

    private final static String rocketmq_connect = "rocketmq.connect:";
    private final static String split_semicolon = ";";
    private final static String split_kv = "=";


    /**
     * MetricName to string
     *
     * @param name
     * @return
     */
    public static String metricNameToString(MetricName name) {
        StringBuilder sb = new StringBuilder(rocketmq_connect)
                .append(name.group())
                .append(split_semicolon)
                .append(name.name());
        for (Map.Entry<String, String> entry : name.tags().entrySet()) {
            sb.append(split_semicolon)
                    .append(entry.getKey())
                    .append(split_kv)
                    .append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * string to MetricName
     *
     * @param name
     * @return
     */
    public static MetricName stringToMetricName(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Metric name str is empty");
        }
        String[] splits = name
                .replace(rocketmq_connect, "")
                .replace(split_kv,split_semicolon)
                .split(split_semicolon);
        return new MetricName(
                splits[0],
                splits[1],
                getTags(Arrays.copyOfRange(splits, 2,splits.length))
        );
    }


    public static Map<String, String> getTags(String... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Map<String, String> tags = new LinkedHashMap<>(keyValue.length / 2);

        for (int i = 0; i < keyValue.length; i += 2)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }

}
