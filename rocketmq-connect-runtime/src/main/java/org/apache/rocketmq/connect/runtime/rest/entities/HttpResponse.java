/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.rest.entities;

import java.util.Map;

/**
 * http response
 * @param <T>
 */
public class HttpResponse<T> {
    private final int status;
    private final Map<String, String> headers;
    private final T body;

    public HttpResponse(int status, Map<String, String> headers, T body) {
        this.status = status;
        this.headers = headers;
        this.body = body;
    }

    public int status() {
        return status;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public T body() {
        return body;
    }
}