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

import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class HttpCallback implements FutureCallback<String> {

    private static final Logger log = LoggerFactory.getLogger(HttpCallback.class);

    private CountDownLatch countDownLatch;

    private boolean isFailed;

    private String msg;

    public HttpCallback(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void completed(String s) {
        countDownLatch.countDown();
    }

    public void failed(final Exception ex) {
        countDownLatch.countDown();
        isFailed = true;
        log.error("http request failed.", ex);
    }

    public void cancelled() {
        countDownLatch.countDown();
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public boolean isFailed() {
        return isFailed;
    }

    public void setFailed(boolean failed) {
        isFailed = failed;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
