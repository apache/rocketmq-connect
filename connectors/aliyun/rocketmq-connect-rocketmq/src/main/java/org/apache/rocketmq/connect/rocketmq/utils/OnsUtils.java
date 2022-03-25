package org.apache.rocketmq.connect.rocketmq.utils;

public class OnsUtils {

    public static String parseEndpoint(String namesrvAddr) {
        final String[] split = namesrvAddr.split("\\.");
        return "ons." + split[1] + ".aliyuncs.com";
    }
}
