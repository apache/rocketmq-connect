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

package org.apache.rocketmq.connect.elasticsearch.connector;

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.io.IOException;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConstant;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

    private ElasticsearchConfig config;

    private RestHighLevelClient restHighLevelClient;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }
        for (ConnectRecord record : sinkRecords) {
            final String indexName = record.getExtension(ElasticsearchConstant.INDEX);
            if (indexName == null || indexName == "") {
                continue;
            }
            final List<Field> fields = record.getSchema().getFields();
            final Struct structData = (Struct) record.getData();
            JSONObject object = new JSONObject();
            for (Field field : fields) {
                object.put(field.getName(), structData.get(field));
            }
            IndexRequest indexRequest = new IndexRequest(indexName);
            indexRequest.source(object.toJSONString(), XContentType.JSON);
            try {
                restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("save data error, indexName:{}, schema:{},  data:{}", indexName, record.getSchema(),  record.getData());
            }
        }

    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = new ElasticsearchConfig();
        this.config.load(keyValue);
        HttpHost httpHost = new HttpHost(config.getElasticsearchHost(), config.getElasticsearchPort());
        Node node = new Node(httpHost);

        RestClientBuilder restClientBuilder = RestClient.builder(node);
        if (config.getUsername() != null && config.getPassword() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    }

    @Override
    public void stop() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
