package org.apache.rocketmq.connect.oss.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimeUtils {
    public static String generateDtm(Long timeStamp,String dtmPattern) {
        Instant instant = Instant.ofEpochMilli(timeStamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai"));
        String dtm = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        int hour = localDateTime.getHour();
        return dtmPattern.replace("{DTM}",dtm)
                .replace("{HH}", String.format("%02d", hour));
    }
}
