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

package org.apache.rocketmq.connect.doris.converter.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.converter.RecordDescriptor;
import org.apache.rocketmq.connect.doris.exception.SchemaChangeException;
import org.apache.rocketmq.connect.doris.service.DorisSystemService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_OK;

public class SchemaChangeManager implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeManager.class);
    private static final String ADD_DDL = "ALTER TABLE %s ADD COLUMN %s %s";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DorisOptions dorisOptions;
    private final DorisSystemService dorisSystemService;

    public SchemaChangeManager(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        this.dorisSystemService = new DorisSystemService(dorisOptions);
    }

    private boolean handleSchemaChange(Map<String, Object> responseMap, String responseEntity) {
        String code = responseMap.getOrDefault("code", "-1").toString();
        if (code.equals("0")) {
            return true;
        } else {
            throw new SchemaChangeException("Failed to schemaChange, response: " + responseEntity);
        }
    }

    public void addColumnDDL(String tableName, RecordDescriptor.FieldDescriptor field) {
        try {
            // check the add column whether exist in table.
            if (dorisSystemService.isColumnExist(
                dorisOptions.getDatabase(), tableName, field.getName())) {
                LOG.warn(
                    "The column {} already exists in table {}, no need to add it again",
                    field.getName(),
                    tableName);
                return;
            }

            String addColumnDDL = buildAddColumnDDL(dorisOptions.getDatabase(), tableName, field);
            boolean status = execute(addColumnDDL, dorisOptions.getDatabase());
            LOG.info(
                "Add missing column for {} table, ddl={}, status={}",
                tableName,
                addColumnDDL,
                status);
        } catch (Exception e) {
            LOG.warn("Failed to add column for {}, cause by: ", tableName, e);
            throw new SchemaChangeException(
                "Failed to add column for " + tableName + ", cause by:", e);
        }
    }

    public static String buildAddColumnDDL(
        String database, String tableName, RecordDescriptor.FieldDescriptor field) {
        String name = field.getName();
        String typeName = field.getTypeName();
        String comment = field.getComment();
        String defaultValue = field.getDefaultValue();

        String addDDL =
            String.format(
                ADD_DDL,
                identifier(database) + "." + identifier(tableName),
                identifier(name),
                typeName);
        if (defaultValue != null) {
            addDDL = addDDL + " DEFAULT " + quoteDefaultValue(defaultValue);
        }
        if (StringUtils.isNotEmpty(comment)) {
            addDDL = addDDL + " COMMENT '" + quoteComment(comment) + "'";
        }
        return addDDL;
    }

    private static String quoteComment(String comment) {
        return comment.replaceAll("'", "\\\\'");
    }

    private static String identifier(String name) {
        return "`" + name + "`";
    }

    private static String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")) {
            return defaultValue;
        }
        return "'" + defaultValue + "'";
    }

    /**
     * execute sql in doris.
     */
    public boolean execute(String ddl, String database)
        throws IOException, IllegalArgumentException {
        if (StringUtils.isEmpty(ddl)) {
            return false;
        }
        LOG.info("Execute SQL: {}", ddl);
        HttpPost httpPost = buildHttpPost(ddl, database);
        String responseEntity = "";
        Map<String, Object> responseMap = handleResponse(httpPost, responseEntity);
        return handleSchemaChange(responseMap, responseEntity);
    }

    public HttpPost buildHttpPost(String ddl, String database)
        throws IllegalArgumentException, IOException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", ddl);
        String requestUrl = String.format(SCHEMA_CHANGE_API, dorisOptions.getHttpUrl(), database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
        return httpPost;
    }

    private Map<String, Object> handleResponse(HttpUriRequest request, String responseEntity) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == HTTP_OK && response.getEntity() != null) {
                responseEntity = EntityUtils.toString(response.getEntity());
                return objectMapper.readValue(responseEntity, Map.class);
            } else {
                throw new SchemaChangeException(
                    "Failed to schemaChange, status: "
                        + statusCode
                        + ", reason: "
                        + reasonPhrase);
            }
        } catch (Exception e) {
            LOG.error("SchemaChange request error,", e);
            throw new SchemaChangeException("SchemaChange request error with " + e.getMessage());
        }
    }

    private String authHeader() {
        return "Basic "
            + new String(
            Base64.encodeBase64(
                (dorisOptions.getUser() + ":" + dorisOptions.getPassword())
                    .getBytes(StandardCharsets.UTF_8)));
    }
}
