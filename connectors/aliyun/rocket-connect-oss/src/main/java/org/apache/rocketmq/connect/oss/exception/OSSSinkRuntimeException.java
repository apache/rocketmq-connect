package org.apache.rocketmq.connect.oss.exception;

import com.aliyuncs.exceptions.ClientException;

public class OSSSinkRuntimeException extends Exception{
    public OSSSinkRuntimeException(ClientException e) {
        super(e);
    }
    public OSSSinkRuntimeException(String errMsg) {
        super(errMsg);
    }
}
