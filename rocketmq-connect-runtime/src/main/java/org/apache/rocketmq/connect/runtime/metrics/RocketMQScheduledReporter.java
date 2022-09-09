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


import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * rocketmq exporter
 */
public class RocketMQScheduledReporter extends ScheduledMetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(RocketMQScheduledReporter.class);

    private final static String METRICS_TOPIC = "metrics.topic";
    private final static String NAMESRV_ADDR= "name.srv.addr";

    private final static String GROUP_ID = "group.id";
    private final static String ACL_ENABLED = "acl.enable";
    private final static String ACCESS_KEY = "access.key";
    private final static String SECRET_KEY = "secret.key";

    private final static String name = "rocketmq-exporter";

    private DefaultMQProducer producer;

    private String topic;

    public RocketMQScheduledReporter(MetricRegistry registry) {
        super(registry, name);
    }

    @Override
    public void reportMetric(SortedMap<MetricName, Gauge> gauges, SortedMap<MetricName, Counter> counters, SortedMap<MetricName, Histogram> histograms, SortedMap<MetricName, Meter> meters, SortedMap<MetricName, Timer> timers) {
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
    private void reportGauges(SortedMap<MetricName, Gauge> gauges) {
        gauges.forEach((name, gauge)->{
            send(name, Double.valueOf(gauge.getValue().toString()));
        });
    }

    /**
     * report counters
     *
     * @param counters
     */
    private void reportCounters(SortedMap<MetricName, Counter> counters) {
        counters.forEach((name, counter)->{
            send(name, Double.valueOf(counter.getCount()));
        });
    }

    /**
     * report histograms
     * @param histograms
     */
    private void reportHistograms(SortedMap<MetricName, Histogram> histograms) {
        histograms.forEach((name, histogram)->{
            send(name, histogramValue(name, histogram));
        });
    }

    /**
     * report meters
     * @param meters
     */
    private void reportMeters(SortedMap<MetricName, Meter> meters) {
        meters.forEach((name, meter)->{
            send(name, meter.getMeanRate());
        });
    }

    /**
     * report timers
     * @param timers
     */
    private void reportTimers(SortedMap<MetricName, Timer> timers) {
        timers.forEach((name, timer)->{
            send(name, timer.getMeanRate());
        });
    }



    @Override
    public void config(Map<String, String> configs) {
        if (!configs.containsKey(METRICS_TOPIC) || !configs.containsKey(NAMESRV_ADDR)){
            throw new IllegalArgumentException("Configuration metrics.topic and name.srv.addr cannot be empty");
        }

        RPCHook rpcHook = null;
        if (configs.containsKey(ACL_ENABLED) && Boolean.valueOf(configs.get(ACL_ENABLED))) {
            if (!configs.containsKey(ACCESS_KEY) || !configs.containsKey(SECRET_KEY)){
                throw new IllegalArgumentException("Configuration access.key and secret.key cannot be empty");
            }
            rpcHook = new AclClientRPCHook(new SessionCredentials(configs.get(ACCESS_KEY), configs.get(SECRET_KEY)));
        }
        producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(configs.get(NAMESRV_ADDR));
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setProducerGroup(configs.get(GROUP_ID));
        producer.setSendMsgTimeout(5000);
        producer.setLanguage(LanguageCode.JAVA);
        this.topic = configs.get(METRICS_TOPIC);

    }

    @Override
    public void start() {
        this.start(10, TimeUnit.SECONDS);
    }

    private void send(MetricName name, Double value){
        try {
            Message message =  new Message();
            message.setTopic(this.topic);
            message.setKeys(name.getStr());
            message.setBody(value.toString().getBytes("UTF-8"));
            producer.send(message);
        } catch (Exception e) {
            log.error("Send metrics error", e);
        }
    }
}
