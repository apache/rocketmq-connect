package org.apache.rocketmq.connect.http.sink.utils;

import org.apache.commons.lang3.StringUtils;

public class CheckUtils {

    private static final String NULL_CONSTANT = "null";

    public static Boolean checkNull(String check) {
        if (StringUtils.isBlank(check)) {
            return Boolean.TRUE;
        }
        if (StringUtils.isNotBlank(check) && NULL_CONSTANT.equalsIgnoreCase(check)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public static Boolean checkNotNull(String check) {
        if (StringUtils.isNotBlank(check) && !NULL_CONSTANT.equalsIgnoreCase(check)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public static String checkNullReturnDefault(String check) {
        if (NULL_CONSTANT.equalsIgnoreCase(check)) {
            return null;
        }
        return check;
    }

}
