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
package org.apache.rocketmq.connect.http.util;

import org.apache.commons.lang3.StringUtils;

public class CheckUtils {

    private static final String NULL_CONSTANT = "null";

    public static Boolean checkNull(String check) {
        if (StringUtils.isBlank(check)) {
            return Boolean.TRUE;
        }
        if (StringUtils.isNotBlank(check) && NULL_CONSTANT.equalsIgnoreCase(check)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public static Boolean checkNotNull(String check) {
        if (StringUtils.isNotBlank(check) && !NULL_CONSTANT.equalsIgnoreCase(check)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public static String checkNullReturnDefault(String check) {
        if (NULL_CONSTANT.equalsIgnoreCase(check)) {
            return null;
        }
        return check;
    }

}
