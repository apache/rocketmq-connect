package org.apache.rocketmq.connect.http.sink.enums;

public enum MethodEnum {

    /**
     * GET
     */
    GET("GET"),
    POST("POST"),
    DELETE("DELETE"),
    PUT("PUT"),
    HEAD("HEAD"),
    TRACE("TRACE"),
    PATCH("PATCH"),
    OPTIONS("OPTIONS");
    private final String method;

    MethodEnum(String method) {
        this.method = method;
    }

    public String getMethod() {
        return method;
    }
}
