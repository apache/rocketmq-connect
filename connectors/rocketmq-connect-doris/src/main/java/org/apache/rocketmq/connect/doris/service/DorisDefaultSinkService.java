/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.service;

import com.codahale.metrics.MetricRegistry;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.connection.ConnectionProvider;
import org.apache.rocketmq.connect.doris.connection.JdbcConnectionProvider;
import org.apache.rocketmq.connect.doris.metrics.DorisConnectMonitor;
import org.apache.rocketmq.connect.doris.metrics.MetricsJmxReporter;
import org.apache.rocketmq.connect.doris.utils.ConnectRecordUtil;
import org.apache.rocketmq.connect.doris.writer.CopyIntoWriter;
import org.apache.rocketmq.connect.doris.writer.DorisWriter;
import org.apache.rocketmq.connect.doris.writer.StreamLoadWriter;
import org.apache.rocketmq.connect.doris.writer.load.LoadModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisDefaultSinkService implements DorisSinkService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDefaultSinkService.class);
    private final ConnectionProvider conn;
    private final Map<String, DorisWriter> writer;
    private final DorisOptions dorisOptions;
    private final MetricsJmxReporter metricsJmxReporter;
    private final DorisConnectMonitor connectMonitor;

    DorisDefaultSinkService(KeyValue config) {
        this.dorisOptions = new DorisOptions(config);
        this.writer = new HashMap<>();
        this.conn = new JdbcConnectionProvider(dorisOptions);
        MetricRegistry metricRegistry = new MetricRegistry();
        this.metricsJmxReporter = new MetricsJmxReporter(metricRegistry, dorisOptions.getName());
        this.connectMonitor =
            new DorisConnectMonitor(
                dorisOptions.isEnableCustomJMX(),
                dorisOptions.getTaskId(),
                this.metricsJmxReporter);
    }

    public void startService(String topic) {
        LoadModel loadModel = dorisOptions.getLoadModel();
        DorisWriter dorisWriter =
            LoadModel.COPY_INTO.equals(loadModel)
                ? new CopyIntoWriter(
                topic, dorisOptions, conn, connectMonitor)
                : new StreamLoadWriter(
                topic, dorisOptions, conn, connectMonitor);
        writer.put(topic, dorisWriter);
        metricsJmxReporter.start();
    }

    @Override
    public void insert(final List<ConnectRecord> records) {
        for (ConnectRecord record : records) {
            if (Objects.isNull(record.getData())) {
                RecordPartition partition = record.getPosition().getPartition();
                LOG.debug(
                    "Null valued record from topic={} brokerName={} queueId={} and offset={} was skipped",
                    ConnectRecordUtil.getTopicName(partition),
                    ConnectRecordUtil.getBrokerName(partition),
                    ConnectRecordUtil.getQueueId(partition),
                    ConnectRecordUtil.getQueueOffset(record.getPosition().getOffset()));
                continue;
            }
            insert(record);
        }
        // check all sink writer to see if they need to be flushed
        for (DorisWriter writer : writer.values()) {
            // Time based flushing
            if (writer.shouldFlush()) {
                writer.flushBuffer();
            }
        }
    }

    @Override
    public void insert(final ConnectRecord record) {
        String topicName = ConnectRecordUtil.getTopicName(record.getPosition().getPartition());
        if (!writer.containsKey(topicName)) {
            // todo startTask can be initialized in DorisSinkTask, and SinkTask needs to support the initialization method for each RecordPartition.
            startService(topicName);
        }
        writer.get(topicName).insert(record);
    }

    @Override
    public void commit(Map<RecordPartition, RecordOffset> currentOffsets) {
        currentOffsets.keySet()
            .forEach(
                rp -> {
                    String topicName = ConnectRecordUtil.getTopicName(rp);
                    writer.get(topicName).commit();
                });
    }

    @Override
    public int getDorisWriterSize() {
        return writer.size();
    }
}
