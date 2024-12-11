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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.exception.CopyLoadException;
import org.apache.rocketmq.connect.doris.exception.UploadException;
import org.apache.rocketmq.connect.doris.model.BaseResponse;
import org.apache.rocketmq.connect.doris.model.CopyIntoResp;
import org.apache.rocketmq.connect.doris.model.LoadOperation;
import org.apache.rocketmq.connect.doris.utils.BackoffAndRetryUtils;
import org.apache.rocketmq.connect.doris.utils.HttpPostBuilder;
import org.apache.rocketmq.connect.doris.utils.HttpPutBuilder;
import org.apache.rocketmq.connect.doris.utils.HttpUtils;
import org.apache.rocketmq.connect.doris.writer.CopySQLBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyLoad extends DataLoad {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CopyLoad.class);
    public static final int SUCCESS = 0;
    public static final String FAIL = "1";
    private static final Pattern COMMITTED_PATTERN =
        Pattern.compile(
            "errCode = 2, detailMessage = No files can be copied, matched (\\d+) files, "
                + "filtered (\\d+) files because files may be loading or loaded");
    private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
    private static final String COMMIT_PATTERN = "http://%s/copy/query";
    private final String loadUrlStr;
    private final String hostPort;
    private final DorisOptions dorisOptions;
    private final CloseableHttpClient httpClient;

    public CopyLoad(String database, String tableName, DorisOptions dorisOptions) {
        this(
            database,
            tableName,
            dorisOptions,
            new HttpUtils(dorisOptions).getHttpClient());
    }

    public CopyLoad(
        String database,
        String tableName,
        DorisOptions dorisOptions,
        CloseableHttpClient httpClient) {
        this.database = database;
        this.table = tableName;
        this.hostPort = dorisOptions.getUrls() + ":" + dorisOptions.getHttpPort();
        this.loadUrlStr = String.format(UPLOAD_URL_PATTERN, hostPort);
        this.dorisOptions = dorisOptions;
        this.httpClient = httpClient;
    }

    public void uploadFile(String fileName, String value) {
        String address = getUploadAddress(fileName);
        upLoadFile(address, value, fileName);
    }

    /**
     * execute copy into
     */
    public boolean executeCopy(List<String> fileList) {
        long start = System.currentTimeMillis();
        CopySQLBuilder copySQLBuilder =
            new CopySQLBuilder(database, table, fileList, dorisOptions.isEnableDelete());
        String copySQL = copySQLBuilder.buildCopySQL();
        LOG.info("build copy SQL is {}", copySQL);
        Map<String, String> params = new HashMap<>();
        params.put("sql", copySQL);
        try {
            BackoffAndRetryUtils.backoffAndRetry(
                LoadOperation.EXECUTE_COPY,
                () -> {
                    HttpPostBuilder postBuilder = new HttpPostBuilder();
                    postBuilder
                        .setUrl(String.format(COMMIT_PATTERN, hostPort))
                        .baseAuth(dorisOptions.getUser(), dorisOptions.getPassword())
                        .setEntity(
                            new StringEntity(OBJECT_MAPPER.writeValueAsString(params)));

                    try (CloseableHttpResponse response =
                             httpClient.execute(postBuilder.build())) {
                        final int statusCode = response.getStatusLine().getStatusCode();
                        final String reasonPhrase = response.getStatusLine().getReasonPhrase();
                        String loadResult = "";
                        if (statusCode != 200) {
                            LOG.warn(
                                "commit failed with status {} {}, reason {}",
                                statusCode,
                                hostPort,
                                reasonPhrase);
                            throw new CopyLoadException(
                                "commit file failed, cause by: " + reasonPhrase);
                        } else if (response.getEntity() != null) {
                            loadResult = EntityUtils.toString(response.getEntity());
                            boolean success = handleCommitResponse(loadResult);
                            if (success) {
                                LOG.info(
                                    "commit success cost {}ms, response is {}",
                                    System.currentTimeMillis() - start,
                                    loadResult);
                                return true;
                            }
                        }
                        LOG.error("commit failed, cause by: " + loadResult);
                        throw new CopyLoadException("commit failed, cause by: " + loadResult);
                    }
                });
        } catch (Exception e) {
            String errMsg = "failed to execute copy, sql=" + copySQL;
            throw new CopyLoadException(errMsg, e);
        }
        return true;
    }

    public boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse<CopyIntoResp> baseResponse =
            OBJECT_MAPPER.readValue(
                loadResult, new TypeReference<BaseResponse<CopyIntoResp>>() {
                });
        if (baseResponse.getCode() == SUCCESS) {
            CopyIntoResp dataResp = baseResponse.getData();
            if (FAIL.equals(dataResp.getDataCode())) {
                LOG.error("copy into execute failed, reason:{}", loadResult);
                return false;
            } else {
                Map<String, String> result = dataResp.getResult();
                if (!result.get("state").equals("FINISHED") && !isCommitted(result.get("msg"))) {
                    LOG.error("copy into load failed, reason:{}", loadResult);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            LOG.error("commit failed, reason:{}", loadResult);
            return false;
        }
    }

    public static boolean isCommitted(String msg) {
        return COMMITTED_PATTERN.matcher(msg).matches();
    }

    /**
     * Upload File
     */
    public void upLoadFile(String address, String value, String fileName) {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder
            .setUrl(address)
            .addCommonHeader()
            .setEntity(new ByteArrayEntity(value.getBytes(StandardCharsets.UTF_8)));
        try {
            BackoffAndRetryUtils.backoffAndRetry(
                LoadOperation.UPLOAD_FILE,
                () -> {
                    long start = System.currentTimeMillis();
                    try (CloseableHttpResponse response =
                             httpClient.execute(putBuilder.build())) {
                        final int statusCode = response.getStatusLine().getStatusCode();
                        if (statusCode != 200) {
                            String result =
                                response.getEntity() == null
                                    ? null
                                    : EntityUtils.toString(response.getEntity());
                            LOG.error("upload file {} error, response {}", fileName, result);
                            throw new UploadException("upload file error: " + fileName);
                        }
                        LOG.info(
                            "upload file success cost {}ms",
                            System.currentTimeMillis() - start);
                        return true;
                    }
                });
        } catch (Exception e) {
            String errMsg = "Failed to upload file, filename=" + fileName + ", address=" + address;
            throw new UploadException(errMsg, e);
        }
    }

    /**
     * Get the redirected s3 address
     */
    public String getUploadAddress(String fileName) {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder
            .setUrl(loadUrlStr)
            .addFileName(fileName)
            .addCommonHeader()
            .setEmptyEntity()
            .baseAuth(dorisOptions.getUser(), dorisOptions.getPassword());

        AtomicReference<String> uploadAddress = new AtomicReference<>();
        try {
            BackoffAndRetryUtils.backoffAndRetry(
                LoadOperation.GET_UPLOAD_ADDRESS,
                () -> {
                    try (CloseableHttpResponse execute =
                             httpClient.execute(putBuilder.build())) {
                        int statusCode = execute.getStatusLine().getStatusCode();
                        String reason = execute.getStatusLine().getReasonPhrase();
                        if (statusCode == 307) {
                            Header location = execute.getFirstHeader("location");
                            uploadAddress.set(location.getValue());
                            LOG.info("redirect to s3:{}", uploadAddress.get());
                            return true;
                        }
                        HttpEntity entity = execute.getEntity();
                        String result = entity == null ? null : EntityUtils.toString(entity);
                        LOG.error(
                            "Failed get the redirected address, status {}, reason {}, response {}",
                            statusCode,
                            reason,
                            result);
                        throw new UploadException("Could not get the redirected address.");
                    }
                });
        } catch (Exception e) {
            String errMsg =
                "Failed to get redirected upload address, fileName="
                    + fileName
                    + ", loadUrlStr="
                    + loadUrlStr;
            throw new UploadException(errMsg, e);
        }
        return uploadAddress.get();
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error("Closing httpClient failed.", e);
                throw new RuntimeException("Closing httpClient failed.", e);
            }
        }
    }
}
