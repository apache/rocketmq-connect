package org.apache.rocketmq.connect.oss.common.cloudEvent;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CloudEventSendResult {
    private String message;
    private Exception exception;
    private ResultStatus status;

    public CloudEventSendResult() {
    }

    public CloudEventSendResult(ResultStatus status, String message, Exception exception) {
        this.status = status;
        this.message = message;
        this.exception = exception;
    }

    @Override
    public String toString() {
        return "CloudEventSendResult{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", exception=" + exception +
                '}';
    }

    public enum ResultStatus {
        SUCCESS,
        FAIL,
        PENDING,
    }

}
