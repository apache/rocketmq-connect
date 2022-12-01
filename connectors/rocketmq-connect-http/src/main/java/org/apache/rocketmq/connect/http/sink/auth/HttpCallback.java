package org.apache.rocketmq.connect.http.sink.auth;

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
