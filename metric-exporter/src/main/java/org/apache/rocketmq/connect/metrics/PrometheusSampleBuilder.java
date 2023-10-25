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

import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.samplebuilder.SampleBuilder;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class PrometheusSampleBuilder implements SampleBuilder {
    private static final List<String> SOURCE_TASK_LABEL_NAMES = Arrays.asList("metric_group", "data_type", "connector", "task");

    @Override
    public Collector.MetricFamilySamples.Sample createSample(String dropwizardName, String nameSuffix,
        List<String> additionalLabelNames, List<String> additionalLabelValues, double value) {
        String suffix = nameSuffix == null ? "" : nameSuffix;
        List<String> labelValues = sanitizeLabelValues(dropwizardName);
        return new Collector.MetricFamilySamples.Sample(sanitizeMetricName(dropwizardName + suffix), SOURCE_TASK_LABEL_NAMES, labelValues, value);
    }

    public String sanitizeMetricName(String dropwizardName) {
        return dropwizardName.split(":")[1].split(",")[1].replaceAll("-", "_");
    }

    public List<String> sanitizeLabelValues(String dropwizardName) {
        String[] var = dropwizardName.split(":");
        String[] split = var[1].split(",");

        String metricGroup = split[0];
        String metricName = split[1].replaceAll("-", "_");
        String dateType = split[2];
        String var3 = split[3];
        String connectorName = StringUtils.EMPTY;
        if (!StringUtils.equals(var3, "")) {
            connectorName = var3.substring(var3.indexOf("=") + 1);
        }
        String var4 = split[4];
        String taskid = StringUtils.EMPTY;
        if (!StringUtils.equals(var4, "")) {
            taskid = var4.substring(var4.indexOf("=") + 1);
        }
        return Arrays.asList(metricGroup, dateType, connectorName, taskid);
    }
}
