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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.exception.ConnectedFailedException;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.exception.SchemaChangeException;
import org.apache.rocketmq.connect.doris.model.BackendV2;
import org.apache.rocketmq.connect.doris.model.LoadOperation;
import org.apache.rocketmq.connect.doris.model.doris.Schema;
import org.apache.rocketmq.connect.doris.utils.BackoffAndRetryUtils;
import org.slf4j.Logger;

public class RestService {

    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";
    private static final String TABLE_SCHEMA_API = "http://%s/api/%s/%s/_schema";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * get Doris BE nodes to request.
     *
     * @param options configuration of request
     * @param logger  slf4j logger
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    public static List<BackendV2.BackendRowV2> getBackendsV2(DorisOptions options, Logger logger) {
        List<String> feNodeList = options.getHttpUrls();
        if (options.isAutoRedirect()) {
            return convert(feNodeList);
        }

        for (String feNode : feNodeList) {
            try {
                String beUrl = "http://" + feNode + BACKENDS_V2;
                HttpGet httpGet = new HttpGet(beUrl);
                String response = send(options, httpGet, logger);
                logger.info("Backend Info:{}", response);
                List<BackendV2.BackendRowV2> backends = parseBackendV2(response, logger);
                return backends;
            } catch (ConnectedFailedException e) {
                logger.info(
                    "Doris FE node {} is unavailable: {}, Request the next Doris FE node",
                    feNode,
                    e.getMessage());
            }
        }
        String errMsg = "No Doris FE is available, please check configuration";
        logger.error(errMsg);
        throw new DorisException(errMsg);
    }

    /**
     * When the user turns on redirection, there is no need to explicitly obtain the be list, just
     * treat the fe list as the be list.
     */
    private static List<BackendV2.BackendRowV2> convert(List<String> feNodeList) {
        List<BackendV2.BackendRowV2> nodeList = new ArrayList<>();
        for (String node : feNodeList) {
            String[] split = node.split(":");
            nodeList.add(BackendV2.BackendRowV2.of(split[0], Integer.parseInt(split[1]), true));
        }
        return nodeList;
    }

    /**
     * send request to Doris FE and get response json string.
     *
     * @param options configuration of request
     * @param request {@link HttpRequestBase} real request
     * @param logger  {@link Logger}
     * @return Doris FE response in json string
     * @throws ConnectedFailedException throw when cannot connect to Doris FE
     */
    private static String send(DorisOptions options, HttpRequestBase request, Logger logger)
        throws ConnectedFailedException {
        int connectTimeout = options.getRequestConnectTimeoutMs();
        int socketTimeout = options.getRequestReadTimeoutMs();
        logger.trace(
            "connect timeout set to '{}'. socket timeout set to '{}'.",
            connectTimeout,
            socketTimeout);

        RequestConfig requestConfig =
            RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();

        request.setConfig(requestConfig);
        logger.info(
            "Send request to Doris FE '{}' with user '{}'.",
            request.getURI(),
            options.getUser());
        int statusCode = -1;
        AtomicReference<String> result = new AtomicReference<>();
        try {
            BackoffAndRetryUtils.backoffAndRetry(
                LoadOperation.SEND_REQUEST_TO_DORIS,
                () -> {
                    logger.debug("doris request {}.", request.getURI());
                    try {
                        String response = null;
                        if (request instanceof HttpGet) {
                            response =
                                getConnectionGet(
                                    request,
                                    options.getUser(),
                                    options.getPassword(),
                                    logger);
                        } else {
                            response =
                                getConnectionPost(
                                    request,
                                    options.getUser(),
                                    options.getPassword(),
                                    logger);
                        }
                        if (Objects.isNull(response)) {
                            logger.warn(
                                "Failed to get response from Doris FE {}, http code is {}",
                                request.getURI(),
                                statusCode);
                            throw new ConnectedFailedException(
                                "Failed to get response from Doris FE {"
                                    + request.getURI()
                                    + "}, http code is {"
                                    + statusCode
                                    + "}");
                        }
                        logger.trace(
                            "Success get response from Doris FE: {}, response is: {}.",
                            request.getURI(),
                            response);
                        // Handle the problem of inconsistent data format returned by http v1
                        // and v2
                        Map map = OBJECT_MAPPER.readValue(response, Map.class);
                        if (map.containsKey("code") && map.containsKey("msg")) {
                            Object data = map.get("data");
                            result.set(OBJECT_MAPPER.writeValueAsString(data));
                        } else {
                            result.set(response);
                        }
                        return true;
                    } catch (IOException e) {
                        logger.warn(
                            "Failed to connect doris, requestUri={}", request.getURI(), e);
                        throw new ConnectedFailedException(
                            "Failed to connect doris, requestUri=" + request.getURI(), e);
                    }
                });
        } catch (Exception e) {
            logger.error("Connect to doris {} failed.", request.getURI(), e);
            throw new ConnectedFailedException(
                "Failed to connect doris request uri=" + request.getURI(), statusCode, e);
        }
        return result.get();
    }

    private static String getConnectionGet(
        HttpRequestBase request, String user, String passwd, Logger logger) throws IOException {
        URL realUrl = new URL(request.getURI().toString());
        // open connection
        HttpURLConnection connection = (HttpURLConnection) realUrl.openConnection();
        String authEncoding =
            Base64.getEncoder()
                .encodeToString(
                    String.format("%s:%s", user, passwd)
                        .getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + authEncoding);

        connection.connect();
        connection.setConnectTimeout(request.getConfig().getConnectTimeout());
        connection.setReadTimeout(request.getConfig().getSocketTimeout());
        return parseResponse(connection, logger);
    }

    private static String getConnectionPost(
        HttpRequestBase request, String user, String passwd, Logger logger) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding =
            Base64.getEncoder()
                .encodeToString(
                    String.format("%s:%s", user, passwd)
                        .getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost) request).getEntity().getContent();
        String res = IOUtils.toString(content);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(request.getConfig().getConnectTimeout());
        conn.setReadTimeout(request.getConfig().getSocketTimeout());
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn, logger);
    }

    private static String parseResponse(HttpURLConnection connection, Logger logger)
        throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            logger.warn(
                "Failed to get response from Doris  {}, http code is {}",
                connection.getURL(),
                connection.getResponseCode());
            throw new IOException("Failed to get response from Doris");
        }
        StringBuilder result = new StringBuilder();
        try (Scanner scanner = new Scanner(connection.getInputStream(), "utf-8")) {
            while (scanner.hasNext()) {
                result.append(scanner.next());
            }
            return result.toString();
        }
    }

    private static List<BackendV2.BackendRowV2> parseBackendV2(String response, Logger logger) {
        ObjectMapper mapper = new ObjectMapper();
        BackendV2 backend;
        try {
            backend = mapper.readValue(response, BackendV2.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (backend == null) {
            logger.error("Should not come here.");
            throw new DorisException();
        }
        List<BackendV2.BackendRowV2> backendRows = backend.getBackends();
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * Get table schema from doris.
     */
    public static Schema getSchema(
        DorisOptions dorisOptions, String db, String table, Logger logger) {
        logger.trace("start get " + db + "." + table + " schema from doris.");
        Object responseData = null;
        try {
            String tableSchemaUri =
                String.format(TABLE_SCHEMA_API, dorisOptions.getHttpUrl(), db, table);
            HttpGet httpGet = new HttpGet(tableSchemaUri);
            httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader(dorisOptions));
            Map<String, Object> responseMap = handleResponse(httpGet, logger);
            responseData = responseMap.get("data");
            String schemaStr = OBJECT_MAPPER.writeValueAsString(responseData);
            return OBJECT_MAPPER.readValue(schemaStr, Schema.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new SchemaChangeException("can not parse response schema " + responseData, e);
        }
    }

    private static String authHeader(DorisOptions dorisOptions) {
        return "Basic "
            + new String(
            org.apache.commons.codec.binary.Base64.encodeBase64(
                (dorisOptions.getUser() + ":" + dorisOptions.getPassword())
                    .getBytes(StandardCharsets.UTF_8)));
    }

    private static Map handleResponse(HttpUriRequest request, Logger logger) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == 200 && response.getEntity() != null) {
                String responseEntity = EntityUtils.toString(response.getEntity());
                return OBJECT_MAPPER.readValue(responseEntity, Map.class);
            } else {
                throw new SchemaChangeException(
                    "Failed to schemaChange, status: "
                        + statusCode
                        + ", reason: "
                        + reasonPhrase);
            }
        } catch (Exception e) {
            logger.trace("SchemaChange request error,", e);
            throw new SchemaChangeException("SchemaChange request error with " + e.getMessage());
        }
    }
}
