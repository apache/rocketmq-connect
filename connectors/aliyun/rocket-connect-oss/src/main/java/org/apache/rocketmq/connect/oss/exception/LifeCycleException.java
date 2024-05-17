package org.apache.rocketmq.connect.oss.exception;

public class LifeCycleException extends RuntimeException {

    public LifeCycleException(String msg) {
        super(msg);
    }

    public LifeCycleException(Throwable cause) {
        super(cause);
    }

    public LifeCycleException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
