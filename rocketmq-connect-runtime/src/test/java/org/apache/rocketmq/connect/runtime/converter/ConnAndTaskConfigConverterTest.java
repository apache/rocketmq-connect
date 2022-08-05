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

package org.apache.rocketmq.connect.runtime.converter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class ConnAndTaskConfigConverterTest {

    private ConnAndTaskConfigConverter connAndTaskConfigConverter = new ConnAndTaskConfigConverter();

    @Test
    public void objectToByteTest() {
        ConnAndTaskConfigs connAndTaskConfigs = new ConnAndTaskConfigs();
        Map<String, ConnectKeyValue> connectorConfigs = new HashMap<>();
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("nameSrvAddr", "127.0.0.1:9876");
        connectorConfigs.put("nameSrv", connectKeyValue);
        connAndTaskConfigs.setConnectorConfigs(connectorConfigs);
        final byte[] bytes = connAndTaskConfigConverter.objectToByte(connAndTaskConfigs);
        String expected = "{\"task\":{},\"connector\":{\"nameSrv\":\"{\\\"nameSrvAddr\\\":\\\"127.0.0.1:9876\\\"}\"}}";
        Assertions.assertThat(expected.equals(new String(bytes)));
    }

    @Test
    public void byteToObjectTest() {
        String str = "{\"task\":{},\"connector\":{\"nameSrv\":\"{\\\"nameSrvAddr\\\":\\\"127.0.0.1:9876\\\"}\"}}";
        final ConnAndTaskConfigs configs = connAndTaskConfigConverter.byteToObject(str.getBytes(StandardCharsets.UTF_8));
        Assertions.assertThat("127.0.0.1:9876".equals(configs.getConnectorConfigs().get("nameSrv").getString("nameSrvAddr")));
    }
}
