package org.apache.rocketmq.connect.mns.source.utils;

import org.apache.commons.lang3.StringUtils;

public class AliyunMnsUtil {

    private static final int ARRAY_LENGTH = 3;

    public static String parseRegionIdFromEndpoint(String endpoint) {
        String regionId = null;
        if (StringUtils.isBlank(endpoint)) {
            return regionId;
        }
        String[] split = endpoint.split("\\.");
        if (split.length >= ARRAY_LENGTH) {
            regionId = split[2];
        }
        return regionId;
    }

}
