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

package org.apache.rocketmq.connect.doris.writer.commit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.exception.StreamLoadException;
import org.apache.rocketmq.connect.doris.model.LoadOperation;
import org.apache.rocketmq.connect.doris.utils.BackendUtils;
import org.apache.rocketmq.connect.doris.utils.BackoffAndRetryUtils;
import org.apache.rocketmq.connect.doris.utils.HttpPutBuilder;
import org.apache.rocketmq.connect.doris.utils.HttpUtils;
import org.apache.rocketmq.connect.doris.writer.LoadStatus;
import org.apache.rocketmq.connect.doris.writer.ResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisCommitter {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCommitter.class);
    private static final String COMMIT_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CloseableHttpClient httpClient = new HttpUtils().getHttpClient();
    private final BackendUtils backendUtils;
    private final DorisOptions dorisOptions;

    public DorisCommitter(DorisOptions dorisOptions, BackendUtils backendUtils) {
        this.backendUtils = backendUtils;
        this.dorisOptions = dorisOptions;
    }

    public void commit(List<DorisCommittable> dorisCommittables) {
        if (!dorisOptions.enable2PC() || dorisCommittables.isEmpty()) {
            return;
        }
        for (DorisCommittable dorisCommittable : dorisCommittables) {
            try {
                commitTransaction(dorisCommittable);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void commitTransaction(DorisCommittable committable) throws IOException {
        // basic params
        HttpPutBuilder builder =
            new HttpPutBuilder()
                .addCommonHeader()
                .baseAuth(dorisOptions.getUser(), dorisOptions.getPassword())
                .addTxnId(committable.getTxnID())
                .commit();

        AtomicReference<String> hostPort = new AtomicReference<>(committable.getHostPort());
        try {
            BackoffAndRetryUtils.backoffAndRetry(
                LoadOperation.COMMIT_TRANSACTION,
                () -> {
                    // get latest-url
                    LOG.info(
                        "commit txn {} to host {}", committable.getTxnID(), hostPort.get());
                    String url =
                        String.format(COMMIT_PATTERN, hostPort.get(), committable.getDb());
                    HttpPut httpPut = builder.setUrl(url).setEmptyEntity().build();

                    // http execute...
                    try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
                        StatusLine statusLine = response.getStatusLine();
                        if (200 == statusLine.getStatusCode()) {
                            String loadResult = null;
                            if (response.getEntity() != null) {
                                loadResult = EntityUtils.toString(response.getEntity());
                                Map<String, String> res =
                                    objectMapper.readValue(
                                        loadResult,
                                        new TypeReference<
                                            HashMap<String, String>>() {
                                        });
                                if (!res.get("status").equals(LoadStatus.SUCCESS)
                                    && !ResponseUtil.isCommitted(res.get("msg"))) {
                                    throw new StreamLoadException(
                                        "commit transaction failed " + loadResult);
                                }
                            }
                            LOG.info("load result {}", loadResult);
                            return true;
                        }
                        String reasonPhrase = statusLine.getReasonPhrase();
                        LOG.error(
                            "commit failed with {}, reason {}",
                            hostPort.get(),
                            reasonPhrase);
                        hostPort.set(backendUtils.getAvailableBackend());
                        throw new StreamLoadException(
                            "commit failed with {"
                                + hostPort.get()
                                + "}, reason {"
                                + reasonPhrase
                                + "}");
                    } catch (Exception e) {
                        LOG.error("commit transaction failed, to retry, {}", e.getMessage());
                        hostPort.set(backendUtils.getAvailableBackend());
                        throw new StreamLoadException("commit transaction failed.", e);
                    }
                });
        } catch (Exception e) {
            LOG.error("commit transaction error:", e);
            throw new StreamLoadException("commit transaction error: " + e);
        }
    }
}
