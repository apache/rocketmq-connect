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
package org.apache.rocketmq.connect.http.auth;

import com.google.common.collect.Maps;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.constant.HttpHeaderConstant;
import org.apache.rocketmq.connect.http.entity.HttpAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpBasicAuthParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BasicAuthImpl implements Auth {
    private static final Logger log = LoggerFactory.getLogger(BasicAuthImpl.class);
    private HttpAuthParameters httpAuthParameters;

    public BasicAuthImpl(HttpAuthParameters httpAuthParameters) {
        this.httpAuthParameters = httpAuthParameters;
    }

    @Override
    public Map<String, String> auth() {
        Map<String, String> headMap = Maps.newHashMap();
        try {
            HttpBasicAuthParameters httpBasicAuthParameters = httpAuthParameters.getHttpBasicAuthParameters();
            if (httpBasicAuthParameters != null && StringUtils.isNotBlank(httpBasicAuthParameters.getUsername()) && StringUtils.isNotBlank(httpBasicAuthParameters.getPassword())) {
                String authorizationValue = httpBasicAuthParameters.getUsername() + ":" + httpBasicAuthParameters.getPassword();
                headMap.put(HttpHeaderConstant.AUTHORIZATION, "Basic " + Base64.encode(authorizationValue.getBytes(StandardCharsets.UTF_8)));
            }
        } catch (Exception e) {
            log.error("BasicAuthImpl | auth | error => ", e);
        }
        return headMap;
    }
}
