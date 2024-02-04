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
package org.apache.rocketmq.connect.http.constant;

public enum AuthTypeEnum {

    /**
     * BASIC
     */
    BASIC("BASIC_AUTH"),
    /**
     * OAUTH_CLIENT_CREDENTIALS
     */
    OAUTH_CLIENT_CREDENTIALS("OAUTH_AUTH"),
    /**
     * API_KEY
     */
    API_KEY("API_KEY_AUTH"),
    NONE("NONE");
    private final String authType;

    AuthTypeEnum(String authType) {
        this.authType = authType;
    }

    public String getAuthType() {
        return authType;
    }

    public static AuthTypeEnum parse(String authType) {
        for (AuthTypeEnum authTypeEnum : AuthTypeEnum.values()) {
            if (authTypeEnum.getAuthType().equals(authType)) {
                return authTypeEnum;
            }
        }
        return null;
    }
}
