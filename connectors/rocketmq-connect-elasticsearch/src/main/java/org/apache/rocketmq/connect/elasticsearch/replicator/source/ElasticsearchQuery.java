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

package org.apache.rocketmq.connect.elasticsearch.replicator.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.RecordOffset;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConstant;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchQuery {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ElasticsearchReplicator replicator;

    private ElasticsearchConfig config;

    private RestHighLevelClient client;

    private RestClient restClient;

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    public ElasticsearchQuery(ElasticsearchReplicator replicator) {
        this.replicator = replicator;
        this.config = replicator.getConfig();
        HttpHost httpHost = new HttpHost(config.getElasticsearchHost(), config.getElasticsearchPort());
        Node node = new Node(httpHost);
        RestClientBuilder restClientBuilder = RestClient.builder(node);
        if (config.getUsername() != null && config.getPassword() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        restClient = restClientBuilder.build();
        client = new RestHighLevelClient(restClientBuilder);
    }

    public void start(RecordOffset recordOffset, KeyValue keyValue) {
        String  scrollId = null;
        while (true) {
            if (scrollId == null) {
                try {
                    final SearchResponse searchResponse = this.searchData(recordOffset, keyValue);
                    final SearchHit[] hits = searchResponse.getHits().getHits();
                    if (hits == null || hits.length < 1) {
                        break;
                    }
                    for (SearchHit hit : hits) {
                        replicator.getQueue().add(hit);
                    }
                    scrollId = searchResponse.getScrollId();

                } catch (Exception e) {
                    logger.error("query Elasticsearch server failed", e);
                    throw new RuntimeException(e);
                }
            }

            try {
                final SearchResponse searchResponse = scrollSearchData(scrollId);
                final SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits == null || hits.length < 1) {
                    break;
                }
                for (SearchHit hit : hits) {
                    replicator.getQueue().add(hit);
                }
                scrollId = searchResponse.getScrollId();
            } catch (Exception e) {
                logger.error("scroll query Elasticsearch server occur error", e);
                throw new RuntimeException(e);
            }
        }

    }

    private SearchResponse searchData(RecordOffset recordOffset, KeyValue keyValue) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(keyValue.getString(ElasticsearchConstant.INDEX));
        final String shard = keyValue.getString(ElasticsearchConstant.PRIMARY_SHARD);
        if (shard != null) {
            searchRequest.preference("_shards:" + shard);
        }
        searchRequest.scroll(TimeValue.timeValueMinutes(1));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(keyValue.getString(ElasticsearchConstant.INCREMENT_FIELD))
            .gte(keyValue.getString(ElasticsearchConstant.INCREMENT));
        if (recordOffset != null && recordOffset.getOffset() != null && recordOffset.getOffset().size() > 0) {
            final Long offsetValue = (Long) recordOffset.getOffset().get(keyValue.getString(ElasticsearchConstant.INDEX) + ElasticsearchConstant.ES_POSITION);
            rangeQueryBuilder = rangeQueryBuilder.gte(offsetValue);
        }
        searchSourceBuilder.query(rangeQueryBuilder);
        searchSourceBuilder.from(0).size(200);
        searchRequest.source(searchSourceBuilder);
        final SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return searchResponse;
    }

    private SearchResponse scrollSearchData(String scrollId) {
        try {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
            searchScrollRequest.scrollId(scrollId);
            final SearchResponse searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            return searchResponse;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            client.close();
            executorService.shutdown();
        } catch (IOException e) {
            logger.error("close RestHighLevelClient occur error", e);
        }
    }

}
