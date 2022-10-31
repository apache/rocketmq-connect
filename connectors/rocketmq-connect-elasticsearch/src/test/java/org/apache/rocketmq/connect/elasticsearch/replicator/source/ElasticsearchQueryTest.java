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

import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

public class ElasticsearchQueryTest {

    private ElasticsearchQuery query;

    private ElasticsearchReplicator replicator;

    private ElasticsearchConfig config;

    @Before
    public void before() {
        config = new ElasticsearchConfig();
        config.setElasticsearchHost("localhost");
        config.setElasticsearchPort(9200);
        config.setIndex("{\"index_connect\":{\"id\":1},\"index_connect2\":{\"id\":2}}");
        replicator = new ElasticsearchReplicator(config);
        query = new ElasticsearchQuery(replicator);
    }

    @Test
    public void startTest() {
        Assertions.assertThatCode(() -> query.start(null, new DefaultKeyValue())).doesNotThrowAnyException();
    }
}
