package com.aliyun.rocketmq.connect.mns.source.enums;

public enum CloudEventsEnum {

    /**
     * id
     */
    CE_ID("id"),

    CE_SOURCE("source"),
    CE_SPECVERSION("specversion", "1.0"),
    CE_TYPE("type"),
    CE_DATACONTENTTYPE("datacontenttype", "application/json; charset=utf-8"),
    CE_SUBJECT("subject"),
    CE_ALIYUNACCOUNTID("aliyunaccountid");
    private String code;

    private String defaultValue;

    CloudEventsEnum(String code) {
        this.code = code;
    }

    CloudEventsEnum(String code, String defaultValue) {
        this.code = code;
        this.defaultValue = defaultValue;
    }

    public String getCode() {
        return code;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

}
