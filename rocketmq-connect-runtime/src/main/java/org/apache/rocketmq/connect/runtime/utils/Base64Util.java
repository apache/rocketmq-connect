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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Base64;

/**
 * base64 utils
 */
public class Base64Util {

    /**
     * encode
     *
     * @param in
     * @return
     */
    public static String base64Encode(byte[] in) {
        if (in == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(in);
    }

    /**
     * decode
     *
     * @param in
     * @return
     */
    public static byte[] base64Decode(String in) {
        if (StringUtils.isEmpty(in)) {
            return null;
        }
        return Base64.getDecoder().decode(in);
    }
}
