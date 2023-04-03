/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.replicator;

import java.lang.reflect.Field;
import org.apache.rocketmq.replicator.config.RmqConnectorConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnitRunner;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.replicator.config.ConfigDefine;

@RunWith(MockitoJUnitRunner.class)
public class RmqSourceReplicatorTest {

    @Test
    public void testGenerateTopic() throws NoSuchFieldException {
        RmqSourceReplicator rmqSourceReplicator = Mockito.spy(RmqSourceReplicator.class);

        RmqConnectorConfig config = new RmqConnectorConfig();
        KeyValue kv = new DefaultKeyValue();
        kv.put(ConfigDefine.CONN_TOPIC_RENAME_FMT, "${topic}.replica");
        config.init(kv);

        Field field = RmqSourceReplicator.class.getDeclaredField("replicatorConfig");
        FieldSetter.setField(rmqSourceReplicator, field, config);
        String dstTopic = rmqSourceReplicator.generateTargetTopic("dest");
        assertThat(dstTopic).isEqualTo("dest.replica");
    }
}
