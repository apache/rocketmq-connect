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

package org.apache.rocketmq.connect.iotdb.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.List;
import org.apache.rocketmq.connect.iotdb.config.IotdbConstant;
import org.junit.Assert;
import org.junit.Test;

public class IotdbSourceConnectorTest {

    @Test
    public void taskConfigsTest(){
        IotdbSourceConnector connector = new IotdbSourceConnector();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(IotdbConstant.IOTDB_HOST, "localhost");
        keyValue.put(IotdbConstant.IOTDB_PORT, 6667);
        keyValue.put(IotdbConstant.IOTDB_PATHS, "root.ln.wf01.wt01,root.ln.wf01.wt02");
        connector.start(keyValue);
        final List<KeyValue> keyValueList = connector.taskConfigs(2);
        final KeyValue keyValue1 = keyValueList.get(0);
        final KeyValue keyValue2 = keyValueList.get(1);
        Assert.assertEquals("localhost", keyValue1.getString(IotdbConstant.IOTDB_HOST));
        Assert.assertEquals(6667 + "", keyValue1.getInt(IotdbConstant.IOTDB_PORT) + "");
        Assert.assertEquals("root.ln.wf01.wt01", keyValue1.getString(IotdbConstant.IOTDB_PATH));

        Assert.assertEquals("localhost", keyValue2.getString(IotdbConstant.IOTDB_HOST));
        Assert.assertEquals(6667 + "", keyValue2.getInt(IotdbConstant.IOTDB_PORT) + "");
        Assert.assertEquals("root.ln.wf01.wt02", keyValue2.getString(IotdbConstant.IOTDB_PATH));
    }

}
