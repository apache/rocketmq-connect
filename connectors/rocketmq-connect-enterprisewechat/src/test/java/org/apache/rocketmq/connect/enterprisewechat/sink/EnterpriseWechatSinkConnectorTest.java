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

package org.apache.rocketmq.connect.enterprisewechat.sink;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.enterprisewechat.common.SinkConstants;
import org.junit.Test;

public class EnterpriseWechatSinkConnectorTest {
    private EnterpriseWechatSinkConnector connector = new EnterpriseWechatSinkConnector();

    // Replace it with your own robot webhook.
    //    private static final String webHook = "http://127.0.0.1";
    private static final String webHook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxx&debug=1";

    @Test
    public void testPut() {
        EnterpriseWechatSinkTask wechatSinkTask = new EnterpriseWechatSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put("webHook", webHook);
        wechatSinkTask.start(keyValue);

        Map<String, Object> map = new HashMap<>();
        map.put("msgtype", "text");

        Map<String, String> map1 = new HashMap<>();
        map1.put("content", "hello world");
        map.put("text", map1);

        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData(JSON.toJSONString(map));
        connectRecordList.add(connectRecord);

        wechatSinkTask.put(connectRecordList);
    }

    @Test
    public void testValidate() {
        KeyValue keyValue = new DefaultKeyValue();
        // put web_hook which need test
        keyValue.put(SinkConstants.WEB_HOOK, webHook);
        connector.validate(keyValue);
    }
}
