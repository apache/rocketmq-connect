package org.apache.rocketmq.connect.eventbridge.sink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    private static final Logger log = LoggerFactory.getLogger(DateUtils.class);
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static synchronized Date getDate(String date, String dateFormat) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
            return simpleDateFormat.parse(date);
        } catch (Exception e) {
            log.error("DateUtils | getDate | error => ", e);
        }
        return null;
    }

}
