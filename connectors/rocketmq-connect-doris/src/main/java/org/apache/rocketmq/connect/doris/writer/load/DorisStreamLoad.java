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

package org.apache.rocketmq.connect.doris.writer.load;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.exception.StreamLoadException;
import org.apache.rocketmq.connect.doris.model.KafkaRespContent;
import org.apache.rocketmq.connect.doris.utils.BackendUtils;
import org.apache.rocketmq.connect.doris.utils.HttpPutBuilder;
import org.apache.rocketmq.connect.doris.utils.HttpUtils;
import org.apache.rocketmq.connect.doris.writer.LoadStatus;
import org.apache.rocketmq.connect.doris.writer.RecordBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisStreamLoad extends DataLoad {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final List<String> DORIS_SUCCESS_STATUS =
        new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));
    private String loadUrl;
    private final DorisOptions dorisOptions;
    private final String topic;
    private String hostPort;
    private final CloseableHttpClient httpClient = new HttpUtils().getHttpClient();
    private final BackendUtils backendUtils;
    private Queue<KafkaRespContent> respContents = new LinkedList<>();
    private final boolean enableGroupCommit;

    public DorisStreamLoad(BackendUtils backendUtils, DorisOptions dorisOptions, String topic) {
        this.database = dorisOptions.getDatabase();
        this.table = dorisOptions.getTopicMapTable(topic);
        this.user = dorisOptions.getUser();
        this.password = dorisOptions.getPassword();
        this.loadUrl = String.format(LOAD_URL_PATTERN, hostPort, database, table);
        this.dorisOptions = dorisOptions;
        this.backendUtils = backendUtils;
        this.topic = topic;
        this.enableGroupCommit = dorisOptions.enableGroupCommit();
    }

    /**
     * execute stream load.
     */
    public void load(String label, RecordBuffer buffer) throws IOException {
        if (enableGroupCommit) {
            label = null;
        }

        refreshLoadUrl(database, table);
        String data = buffer.getData();
        ByteArrayEntity entity = new ByteArrayEntity(data.getBytes(StandardCharsets.UTF_8));
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder
            .setUrl(loadUrl)
            .baseAuth(user, password)
            .setLabel(label)
            .addCommonHeader()
            .setEntity(entity)
            .addHiddenColumns(dorisOptions.isEnableDelete())
            .enable2PC(dorisOptions.enable2PC())
            .addProperties(dorisOptions.getStreamLoadProp());

        if (enableGroupCommit) {
            LOG.info("stream load started with group commit on host {}", hostPort);
        } else {
            LOG.info("stream load started for {} on host {}", label, hostPort);
        }

        LOG.info("stream load started for {} on host {}", label, hostPort);
        try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                LOG.info("load Result {}", loadResult);
                KafkaRespContent respContent =
                    OBJECT_MAPPER.readValue(loadResult, KafkaRespContent.class);
                if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                    String errMsg =
                        String.format(
                            "stream load error: %s, see more in %s",
                            respContent.getMessage(), respContent.getErrorURL());
                    throw new StreamLoadException(errMsg);
                }
                respContent.setDatabase(database);
                respContent.setTable(table);
                respContent.setLastOffset(buffer.getLastOffset());
                respContent.setTopic(topic);
                respContents.add(respContent);
                return;
            }
        } catch (Exception ex) {
            String err;
            if (enableGroupCommit) {
                err = "failed to stream load data with group commit";
            } else {
                err = "failed to stream load data with label: " + label;
            }
            LOG.warn(err, ex);
            throw new StreamLoadException(err, ex);
        }
    }

    public Queue<KafkaRespContent> getKafkaRespContents() {
        return respContents;
    }

    public void setKafkaRespContents(Queue<KafkaRespContent> respContents) {
        this.respContents = respContents;
    }

    public String getHostPort() {
        return hostPort;
    }

    private void refreshLoadUrl(String database, String table) {
        hostPort = backendUtils.getAvailableBackend();
        loadUrl = String.format(LOAD_URL_PATTERN, hostPort, database, table);
    }
}
