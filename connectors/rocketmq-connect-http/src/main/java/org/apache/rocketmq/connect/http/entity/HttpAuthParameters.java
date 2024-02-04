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
package org.apache.rocketmq.connect.http.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class HttpAuthParameters {

    private String authType;
    private HttpBasicAuthParameters httpBasicAuthParameters;
    private HttpApiKeyAuthParameters httpApiKeyAuthParameters;
    private HttpOAuthParameters httpOAuthParameters;
    public HttpAuthParameters(String authType, HttpBasicAuthParameters httpBasicAuthParameters, HttpApiKeyAuthParameters httpApiKeyAuthParameters, HttpOAuthParameters httpOAuthParameters) {
        this.authType = authType;
        this.httpApiKeyAuthParameters = httpApiKeyAuthParameters;
        this.httpBasicAuthParameters = httpBasicAuthParameters;
        this.httpOAuthParameters = httpOAuthParameters;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public HttpBasicAuthParameters getHttpBasicAuthParameters() {
        return httpBasicAuthParameters;
    }

    public void setHttpBasicAuthParameters(HttpBasicAuthParameters httpBasicAuthParameters) {
        this.httpBasicAuthParameters = httpBasicAuthParameters;
    }

    public HttpApiKeyAuthParameters getHttpApiKeyAuthParameters() {
        return httpApiKeyAuthParameters;
    }

    public void setHttpApiKeyAuthParameters(HttpApiKeyAuthParameters httpApiKeyAuthParameters) {
        this.httpApiKeyAuthParameters = httpApiKeyAuthParameters;
    }

    public HttpOAuthParameters getHttpOAuthParameters() {
        return httpOAuthParameters;
    }

    public void setHttpOAuthParameters(HttpOAuthParameters httpOAuthParameters) {
        this.httpOAuthParameters = httpOAuthParameters;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("authType", authType)
                .append("httpApiKeyAuthParameters", httpApiKeyAuthParameters)
                .append("httpBasicAuthParameters", httpBasicAuthParameters)
                .append("httpOAuthParameters", httpOAuthParameters)
                .toString();
    }
}
