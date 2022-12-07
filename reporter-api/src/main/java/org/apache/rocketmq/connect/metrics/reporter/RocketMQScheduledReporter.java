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
package org.apache.rocketmq.connect.metrics.reporter;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.connect.metrics.MetricName;
import org.apache.rocketmq.connect.metrics.ScheduledMetricsReporter;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * rocketmq exporter
 */
public class RocketMQScheduledReporter extends ScheduledMetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(RocketMQScheduledReporter.class);

    private final static String METRICS_TOPIC = "metrics.topic";
    private final static String NAMESRV_ADDR = "name.srv.addr";
    private final static String GROUP_ID = "group.id";
    private final static String ACL_ENABLED = "acl.enable";
    private final static String ACCESS_KEY = "access.key";
    private final static String SECRET_KEY = "secret.key";

    private final static String NAME = "rocketmq-exporter";

    private DefaultMQProducer producer;

    private String topic;

    public RocketMQScheduledReporter(MetricRegistry registry) {
        super(registry, NAME);
    }

    @Override
    public void reportMetric(SortedMap<MetricName, Object> gauges, SortedMap<MetricName, Long> counters, SortedMap<MetricName, Double> histograms, SortedMap<MetricName, Double> meters, SortedMap<MetricName, Timer> timers) {
        reportGauges(gauges);
        reportCounters(counters);
        reportHistograms(histograms);
        reportMeters(meters);
        reportTimers(timers);
    }

    /**
     * report gauges
     *
     * @param gauges
     */
    private void reportGauges(SortedMap<MetricName, Object> gauges) {
        gauges.forEach((name, value) -> {
            send(name, Double.parseDouble(value.toString()));
        });
    }

    /**
     * report counters
     *
     * @param counters
     */
    private void reportCounters(SortedMap<MetricName, Long> counters) {
        counters.forEach((name, value) -> {
            send(name, Double.parseDouble(value.toString()));
        });
    }

    /**
     * report histograms
     *
     * @param histograms
     */
    private void reportHistograms(SortedMap<MetricName, Double> histograms) {
        histograms.forEach((name, value) -> {
            send(name, value);
        });
    }

    /**
     * report meters
     *
     * @param meters
     */
    private void reportMeters(SortedMap<MetricName, Double> meters) {
        meters.forEach((name, value) -> {
            send(name, value);
        });
    }

    /**
     * report timers
     *
     * @param timers
     */
    private void reportTimers(SortedMap<MetricName, Timer> timers) {
        timers.forEach((name, timer) -> {
            send(name, timer.getMeanRate());
        });
    }


    @Override
    public void config(Map<String, String> configs) {
        if (!configs.containsKey(METRICS_TOPIC) || !configs.containsKey(NAMESRV_ADDR)) {
            throw new IllegalArgumentException("Configuration metrics.topic and name.srv.addr cannot be empty");
        }
        this.topic = configs.get(METRICS_TOPIC);
        String groupId = configs.get(GROUP_ID);
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = RocketMQClientUtil.startMQAdminTool(Boolean.valueOf(configs.get(ACL_ENABLED)), configs.get(ACCESS_KEY), configs.get(SECRET_KEY), groupId, configs.get(NAMESRV_ADDR));
            if (!RocketMQClientUtil.topicExist(defaultMQAdminExt, topic)) {
                RocketMQClientUtil.createTopic(defaultMQAdminExt, new TopicConfig(topic));
            }
            if (!RocketMQClientUtil.fetchAllConsumerGroup(defaultMQAdminExt).contains(groupId)) {
                RocketMQClientUtil.createSubGroup(defaultMQAdminExt, groupId);
            }
            this.producer = RocketMQClientUtil.initDefaultMQProducer(Boolean.valueOf(configs.get(ACL_ENABLED)), configs.get(ACCESS_KEY), configs.get(SECRET_KEY), groupId, configs.get(NAMESRV_ADDR));
            this.producer.start();
        } catch (Exception e) {
            log.error("Init config failed ", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }


    @Override
    public void start() {
        this.start(10, TimeUnit.SECONDS);
    }

    private void send(MetricName name, Double value) {
        try {
            Message message = new Message();
            message.setTopic(this.topic);
            message.setKeys(name.getStr());
            message.setBody(value.toString().getBytes(StandardCharsets.UTF_8));
            producer.send(message);
        } catch (Exception e) {
            log.error("Send metrics error", e);
        }
    }

    @Override
    public void close() {
        super.close();
        producer.shutdown();
    }
}
