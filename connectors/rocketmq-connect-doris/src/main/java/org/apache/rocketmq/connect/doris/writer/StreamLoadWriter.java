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

package org.apache.rocketmq.connect.doris.writer;

import com.google.common.annotations.VisibleForTesting;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.connection.ConnectionProvider;
import org.apache.rocketmq.connect.doris.exception.StreamLoadException;
import org.apache.rocketmq.connect.doris.metrics.DorisConnectMonitor;
import org.apache.rocketmq.connect.doris.model.KafkaRespContent;
import org.apache.rocketmq.connect.doris.service.RestService;
import org.apache.rocketmq.connect.doris.utils.BackendUtils;
import org.apache.rocketmq.connect.doris.utils.FileNameUtils;
import org.apache.rocketmq.connect.doris.writer.commit.DorisCommittable;
import org.apache.rocketmq.connect.doris.writer.commit.DorisCommitter;
import org.apache.rocketmq.connect.doris.writer.load.DorisStreamLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use stream-load to import data into doris.
 */
public class StreamLoadWriter extends DorisWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadWriter.class);
    private static final String TRANSACTION_LABEL_PATTEN = "SHOW TRANSACTION FROM %s WHERE LABEL LIKE '";
    private final LabelGenerator labelGenerator;
    private final DorisCommitter dorisCommitter;
    private final DorisStreamLoad dorisStreamLoad;
    private List<DorisCommittable> committableList = new LinkedList<>();

    public StreamLoadWriter(
        String topic,
        DorisOptions dorisOptions,
        ConnectionProvider connectionProvider,
        DorisConnectMonitor connectMonitor) {
        super(topic, dorisOptions, connectionProvider, connectMonitor);
        this.taskId = dorisOptions.getTaskId();
        this.labelGenerator = new LabelGenerator(topic, tableIdentifier);
        BackendUtils backendUtils = BackendUtils.getInstance(dorisOptions, LOG);
        this.dorisCommitter = new DorisCommitter(dorisOptions, backendUtils);
        this.dorisStreamLoad = new DorisStreamLoad(backendUtils, dorisOptions, topic);
        checkDorisTableKey(tableName);
    }

    /**
     * The uniq model has 2pc close by default unless 2pc is forced open.
     */
    @VisibleForTesting
    public void checkDorisTableKey(String tableName) {
        if (dorisOptions.enable2PC()
            && !dorisOptions.force2PC()
            && RestService.isUniqueKeyType(dorisOptions, tableName, LOG)) {
            LOG.info(
                "The {} table type is unique model, the two phase commit default value should be disabled.",
                tableName);
            dorisOptions.setEnable2PC(false);
        }
    }

    public void fetchOffset() {
        Map<String, String> label2Status = fetchLabel2Status();
        long maxOffset = -1;
        for (Map.Entry<String, String> entry : label2Status.entrySet()) {
            String label = entry.getKey();
            String status = entry.getValue();
            if (status.equalsIgnoreCase("VISIBLE")) {
                long offset = FileNameUtils.labelToEndOffset(label);
                if (offset > maxOffset) {
                    maxOffset = offset;
                }
            }
        }
        this.offsetPersistedInDoris.set(maxOffset);
        LOG.info("Init {} offset of {} topic.", maxOffset, topic);
    }

    private Map<String, String> fetchLabel2Status() {
        String queryPatten = String.format(TRANSACTION_LABEL_PATTEN, dorisOptions.getDatabase());
        String tmpTableIdentifier = tableIdentifier.replaceAll("\\.", "_");
        String tmpTopic = topic.replaceAll("\\.", "_");
        String querySQL =
            queryPatten
                + tmpTopic
                + LoadConstants.FILE_DELIM_DEFAULT
                + tmpTableIdentifier
                + LoadConstants.FILE_DELIM_DEFAULT
                + "%'";
        LOG.info("query doris offset by sql: {}", querySQL);
        Map<String, String> label2Status = new HashMap<>();
        try (Connection connection = connectionProvider.getOrEstablishConnection();
             PreparedStatement ps = connection.prepareStatement(querySQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String label = rs.getString("Label");
                String transactionStatus = rs.getString("TransactionStatus");
                label2Status.put(label, transactionStatus);
            }
        } catch (Exception e) {
            LOG.warn(
                "Unable to obtain the label generated when importing data through stream load from doris, "
                    + "causing the doris kafka connector to not guarantee exactly once.",
                e);
            throw new StreamLoadException(
                "Unable to obtain the label generated when importing data through stream load from doris, "
                    + "causing the doris kafka connector to not guarantee exactly once.",
                e);
        }
        return label2Status;
    }

    @Override
    public void insert(ConnectRecord record) {
        initRecord();
        insertRecord(record);
    }

    protected void flush(final RecordBuffer buff) {
        super.flush(buff);
        try {
            String label = labelGenerator.generateLabel(buff.getLastOffset());
            dorisStreamLoad.load(label, buff);
        } catch (IOException e) {
            LOG.warn(
                "Failed to load buffer. buffNumOfRecords={}, lastOffset={}",
                buff.getNumOfRecords(),
                buff.getLastOffset());
            throw new StreamLoadException(e);
        }

        updateFlushedMetrics(buff);
    }

    @Override
    public void commit() {
        // Doris commit
        Queue<KafkaRespContent> respContents = dorisStreamLoad.getKafkaRespContents();
        while (!respContents.isEmpty()) {
            KafkaRespContent respContent = respContents.poll();
            DorisCommittable dorisCommittable =
                new DorisCommittable(
                    dorisStreamLoad.getHostPort(),
                    respContent.getDatabase(),
                    respContent.getTxnId(),
                    respContent.getLastOffset(),
                    respContent.getTopic(),
                    respContent.getTable());
            committableList.add(dorisCommittable);
        }
        dorisStreamLoad.setKafkaRespContents(new LinkedList<>());
        dorisCommitter.commit(committableList);
        updateCommitOffset();
    }

    private void updateCommitOffset() {
        // committedOffset should be updated only when stream load has succeeded.
        committedOffset.set(flushedOffset.get());
        connectMonitor.setCommittedOffset(committedOffset.get() - 1);

        committableList = new LinkedList<>();
    }

}
