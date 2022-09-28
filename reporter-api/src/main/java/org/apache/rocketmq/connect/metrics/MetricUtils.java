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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.metrics.stats.Stat;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * metric utils
 */
public class MetricUtils {

    private final static String ROCKETMQ_CONNECT = "rocketmq.connect:";
    private final static String SPLIT_COMMA = ",";
    private final static String SPLIT_KV = "=";


    /**
     * MetricName to string
     *
     * @param name
     * @return
     */
    public static String metricNameToString(MetricName name) {
        if (StringUtils.isEmpty(name.getType())) {
            name.setType("none");
        }
        StringBuilder sb = new StringBuilder(ROCKETMQ_CONNECT)
                .append(name.getGroup())
                .append(SPLIT_COMMA)
                .append(name.getName())
                .append(SPLIT_COMMA)
                .append(name.getType());


        for (Map.Entry<String, String> entry : name.getTags().entrySet()) {
            sb.append(SPLIT_COMMA)
                    .append(entry.getKey())
                    .append(SPLIT_KV)
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
        String[] splits = name.replace(ROCKETMQ_CONNECT, "").replace(SPLIT_KV, SPLIT_COMMA).split(SPLIT_COMMA);
        return new MetricName(splits[0], splits[1], splits[2], getTags(Arrays.copyOfRange(splits, 3, splits.length))
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


    /**
     * get meter value
     *
     * @param name
     * @param meter
     * @return
     */
    public static Double getMeterValue(MetricName name, Meter meter) {
        if (name.getType().equals(Stat.NoneType.none.name())) {
            throw new IllegalArgumentException("Meter type configuration error");
        }
        Stat.RateType rateType = Stat.RateType.valueOf(name.getType());
        switch (rateType) {
            case MeanRate:
                return meter.getMeanRate();
            case OneMinuteRate:
                return meter.getOneMinuteRate();
            case FiveMinuteRate:
                return meter.getFiveMinuteRate();
            case FifteenMinuteRate:
                return meter.getFifteenMinuteRate();
            default:
                return 0.0;
        }
    }

    /**
     * get histogram value
     *
     * @param name
     * @param histogram
     * @return
     */
    public static Double getHistogramValue(MetricName name, Histogram histogram) {
        if (name.getType().equals(Stat.NoneType.none.name())) {
            throw new IllegalArgumentException("Histogram type configuration error");
        }
        Stat.HistogramType histogramType = Stat.HistogramType.valueOf(name.getType());
        switch (histogramType) {
            case Min:
                return Double.valueOf(histogram.getSnapshot().getMin());
            case Avg:
                return Double.valueOf(histogram.getSnapshot().getMean());
            case Max:
                return Double.valueOf(histogram.getSnapshot().getMax());
            case Percentile_75th:
                return histogram.getSnapshot().get75thPercentile();
            case Percentile_95th:
                return histogram.getSnapshot().get95thPercentile();
            case Percentile_98th:
                return histogram.getSnapshot().get98thPercentile();
            case Percentile_99th:
                return histogram.getSnapshot().get99thPercentile();
            case Percentile_999th:
                return histogram.getSnapshot().get999thPercentile();
            default:
                return 0.0;
        }
    }


}
