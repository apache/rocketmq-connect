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

package org.apache.rocketmq.connect.runtime.utils;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class Base64UtilTest {
    @Test
    public void base64Test() {
        String str = "Hello World";
        final String encodeStr = Base64Util.base64Encode(str.getBytes(StandardCharsets.UTF_8));
        final byte[] decodeByte = Base64Util.base64Decode(encodeStr);
        Assert.assertEquals(str, new String(decodeByte, StandardCharsets.UTF_8));
    }
}
