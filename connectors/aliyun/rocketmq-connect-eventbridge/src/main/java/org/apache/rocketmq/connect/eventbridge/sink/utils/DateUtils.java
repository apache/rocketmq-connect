package org.apache.rocketmq.connect.eventbridge.sink.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtils {
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static Date getDate(String date, String dateFormat) {
        final LocalDateTime parse = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(dateFormat));
        return Date.from(parse.atZone(ZoneId.systemDefault()).toInstant());
    }

}
