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

public class HttpOAuthParameters {

    private String endpoint;
    private String httpMethod;
    private String clientID;
    private String clientSecret;
    private String headerParameters;
    private String queryParameters;
    private String bodyParameters;
    private String timeout;

    public HttpOAuthParameters(String endpoint, String httpMethod, String clientID, String clientSecret, String headerParameters, String queryParameters, String bodyParameters, String timeout) {
        this.endpoint = endpoint;
        this.httpMethod = httpMethod;
        this.clientID = clientID;
        this.clientSecret = clientSecret;
        this.headerParameters = headerParameters;
        this.queryParameters = queryParameters;
        this.bodyParameters = bodyParameters;
        this.timeout = timeout;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String oAuthEndpoint) {
        this.endpoint = oAuthEndpoint;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getHeaderParameters() {
        return headerParameters;
    }

    public void setHeaderParameters(String headerParameters) {
        this.headerParameters = headerParameters;
    }

    public String getQueryParameters() {
        return queryParameters;
    }

    public void setQueryParameters(String queryParameters) {
        this.queryParameters = queryParameters;
    }

    public String getBodyParameters() {
        return bodyParameters;
    }

    public void setBodyParameters(String bodyParameters) {
        this.bodyParameters = bodyParameters;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }
}
