/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.doris.sink;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.connect.doris.connector.DorisSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


/**
 * This example mainly demonstrates how to use stream load to import data
 * Including file type (CSV) and data in JSON format
 */
public class DorisStreamLoader {
    private static final Logger log = LoggerFactory.getLogger(DorisStreamLoader.class);
    // FE IP Address
    private String host;
    // FE port
    private int port;
    // db name
    private String database;
    // table name
    private String table;
    //user name
    private String user;
    //user password
    private String passwd;
    //http path of stream load task submission
    private final String loadUrlWithoutTable;

    private DorisStreamLoader(String host, int port, String database, String user, String passwd) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.passwd = passwd;
        this.loadUrlWithoutTable = String.format("http://%s:%s/api/%s", host, port, database);
    }

    public static DorisStreamLoader create(DorisSinkConfig config) {
        return new DorisStreamLoader(config.getHost(), config.getPort(), config.getDatabase(), config.getUser(), config.getPasswd());
    }

    //Build http client builder
    private final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
        @Override
        protected boolean isRedirectable(String method) {
            // If the connection target is FE, you need to deal with 307 redirect
            return true;
        }
    });

    private String getLoadURL(String table) {
        return String.format("%s/%s/_stream_load", loadUrlWithoutTable, table);
    }


    /**
     * JSON import
     *
     * @param jsonData
     * @throws Exception
     */
    public void loadJson(String jsonData, String table) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(getLoadURL(table));
            put.removeHeaders(HttpHeaders.CONTENT_LENGTH);
            put.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(user, passwd));

            // You can set stream load related properties in the Header, here we set label and column_separator.
            put.setHeader("label", UUID.randomUUID().toString());
            put.setHeader("column_separator", ",");
            put.setHeader("format", "json");

            // Set up the import file. Here you can also use StringEntity to transfer arbitrary data.
            StringEntity entity = new StringEntity(jsonData);
            put.setEntity(entity);
            log.info(put.toString());
            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }

                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(String.format("Stream load failed. status: %s load result: %s", statusCode, loadResult));
                }
                log.info("Get load result: " + loadResult);
            }
        }
    }

    /**
     * Construct authentication information, the authentication method used by doris here is Basic Auth
     *
     * @param username
     * @param password
     * @return
     */
    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }


    public static void main(String[] args) throws Exception {
        DorisStreamLoader loader = new DorisStreamLoader("47.97.179.18", 7030, "db_1", "root", "rocketmq666");
        //json load
        String jsonData = "{\"id\":556393582,\"number\":\"123344\",\"price\":\"23.5\",\"skuname\":\"test\",\"skudesc\":\"zhangfeng_test,test\"}";
        loader.loadJson(jsonData, "doris_test_sink");

    }
}
