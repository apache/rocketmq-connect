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

package org.apache.rocketmq.schema.common;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.UpdateSchemaResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Local schema registry client
 */
public abstract class AbstractLocalSchemaRegistryClient {

    protected final String cluster = "Connect";
    protected final SchemaRegistryClient schemaRegistryClient;
    protected final boolean autoRegisterSchemas;
    protected final boolean useLatestVersion;
    private final Long serdeSchemaRegistryId;
    private final Cache<String, Boolean> cache = CacheBuilder.newBuilder()
            .initialCapacity(1000)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .build();

    public AbstractLocalSchemaRegistryClient(AbstractConverterConfig config) {
        this.schemaRegistryClient = SchemaRegistryClientFactory.newClient(config.getSchemaRegistryUrl(), null);
        this.serdeSchemaRegistryId = config.getSerdeSchemaRegistryId();
        this.autoRegisterSchemas = config.isAutoRegisterSchemas();
        this.useLatestVersion = config.isUseLatestVersion();
    }

    public SchemaResponse autoRegisterOrGetSchema(String namespace, String subject, RegisterSchemaRequest request, ParsedSchema schema) {
        return autoRegisterOrGetSchema(namespace, subject, null, request, schema);
    }

    /**
     * Get registry schema by specify policy
     *
     * @param subject
     * @param schemaName
     * @param request
     * @return
     */
    public SchemaResponse autoRegisterOrGetSchema(String namespace, String subject, String schemaName, RegisterSchemaRequest request, ParsedSchema schema) {
        if (autoRegisterSchemas) {
            return this.autoRegisterSchema(namespace, subject, schemaName, request, schema);
        } else if (serdeSchemaRegistryId != null) {
            throw new RuntimeException("Getting schema based on ID is not supported temporarily");
        } else {
            GetSchemaResponse getSchemaResponse = getSchemaLatestVersion(namespace, subject);
            return SchemaResponse
                    .builder()
                    .subjectName(getSchemaResponse.getSubjectFullName())
                    .schemaName(getSchemaResponse.getSchemaFullName())
                    .recordId(getSchemaResponse.getRecordId())
                    .idl(getSchemaResponse.getIdl())
                    .build();
        }
    }


    /**
     * auto register schema
     *
     * @param subject
     * @param schemaName
     * @param request
     */
    protected SchemaResponse autoRegisterSchema(String namespace, String subject, String schemaName, RegisterSchemaRequest request, ParsedSchema schema) {
        try {
            if (checkSubjectExists(namespace, subject)) {
                // Get all version schema record
                List<SchemaRecordDto> schemaRecordAllVersion = this.schemaRegistryClient.getSchemaListBySubject(cluster, namespace, subject);
                return compareAndGet(namespace, subject, schemaName, request, schemaRecordAllVersion, schema);
            } else {
                RegisterSchemaResponse registerSchemaResponse = this.schemaRegistryClient.registerSchema(cluster, namespace, subject, schemaName, request);
                return SchemaResponse.builder()
                        .subjectName(subject)
                        .schemaName(schemaName)
                        .recordId(registerSchemaResponse.getRecordId())
                        .build();
            }
        } catch (RestClientException | IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * compare and get
     *
     * @param subject
     * @param schemaName
     * @param request
     * @param schemaRecordAllVersion
     * @param schema
     * @return
     */
    protected SchemaResponse compareAndGet(String namespace, String subject, String schemaName, RegisterSchemaRequest request, List<SchemaRecordDto> schemaRecordAllVersion, ParsedSchema schema) {
        SchemaRecordDto matchSchemaRecord = compareAndGet(schemaRecordAllVersion, schemaName, schema);
        if (matchSchemaRecord != null) {
            GetSchemaResponse getSchemaResponse = new GetSchemaResponse();
            getSchemaResponse.setRecordId(matchSchemaRecord.getRecordId());
            return SchemaResponse.builder()
                    .subjectName(getSchemaResponse.getSubjectFullName())
                    .schemaName(getSchemaResponse.getSchemaFullName())
                    .recordId(getSchemaResponse.getRecordId())
                    .idl(request.getSchemaIdl())
                    .build();
        }
        // match is null
        UpdateSchemaRequest updateSchemaRequest = UpdateSchemaRequest.builder()
                .schemaIdl(request.getSchemaIdl())
                .desc(request.getDesc())
                .owner(request.getOwner())
                .build();
        try {
            UpdateSchemaResponse updateSchemaResponse = schemaRegistryClient.updateSchema(cluster, namespace, subject, schemaName, updateSchemaRequest);
            GetSchemaResponse getSchemaResponse = new GetSchemaResponse();
            getSchemaResponse.setRecordId(updateSchemaResponse.getRecordId());
            return SchemaResponse.builder()
                    .subjectName(subject)
                    .schemaName(schemaName)
                    .recordId(updateSchemaResponse.getRecordId())
                    .build();
        } catch (RestClientException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract SchemaRecordDto compareAndGet(List<SchemaRecordDto> schemaRecordAllVersion, String schemaName, ParsedSchema schema);

    /**
     * check subject exists
     *
     * @param subject
     * @return
     */
    public Boolean checkSubjectExists(String namespace, String subject) {
        try {
            schemaRegistryClient.getSchemaBySubject(cluster, namespace, subject);
            return Boolean.TRUE;
        } catch (RestClientException | IOException e) {
            if (e instanceof RestClientException) {
                return Boolean.FALSE;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Get schema latest version
     *
     * @param subject
     * @return
     */
    public GetSchemaResponse getSchemaLatestVersion(String namespace, String subject) {
        try {
            return schemaRegistryClient.getSchemaBySubject(cluster, namespace, subject);
        } catch (RestClientException | IOException e) {
            if (e instanceof RestClientException) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        }
    }


    public GetSchemaResponse getSchemaByRecordId(String namespace, String subject, long recordId) {
        try {
            return schemaRegistryClient.getSchemaByRecordId(cluster, namespace, subject, recordId);
        } catch (RestClientException | IOException e) {
            if (e instanceof RestClientException) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        }
    }


    protected String schemaName(String schemaName) {
        return schemaName.split("/")[1];
    }
}
