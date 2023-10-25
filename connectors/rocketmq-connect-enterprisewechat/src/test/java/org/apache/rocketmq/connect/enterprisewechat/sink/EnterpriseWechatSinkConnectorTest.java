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

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.enterprisewechat.common.SinkConstants;
import org.junit.Test;

public class EnterpriseWechatSinkConnectorTest {
    private EnterpriseWechatSinkConnector connector = new EnterpriseWechatSinkConnector();

    // Replace it with your own robot webhook.
    private static final String webHook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxx&debug=1";

    @Test
    public void testValidate() {
        KeyValue keyValue = new DefaultKeyValue();
        // put webhook which need validation
        keyValue.put(SinkConstants.WEB_HOOK, webHook);
        connector.validate(keyValue);
    }
}
