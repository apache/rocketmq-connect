package org.apache.rocketmq.connect.oss.util;

import lombok.Getter;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigConvertUtils {
    private static final Pattern pattern = Pattern.compile("(\\d+)(\\s*)(h|m|s|H|M|S)?", Pattern.CASE_INSENSITIVE);
    private static final Pattern BATCH_SIZE_PATTERN = Pattern.compile("(\\d+)\\s*(KB|MB|GB)", Pattern.CASE_INSENSITIVE);

    public static long extractBatchSize(String batchSize) {
        Matcher matcher = BATCH_SIZE_PATTERN.matcher(batchSize.trim());
        if (matcher.matches()) {
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            switch (unit) {
                case "KB":
                    return value * 1024;
                case "MB":
                    return value * 1024 * 1024;
                case "GB":
                    return value * 1024 * 1024 * 1024;
                default:
                    throw new IllegalArgumentException("Invalid batch size format: " + batchSize);
            }
        } else {
            throw new IllegalArgumentException("Invalid batch size format: " + batchSize);
        }
    }
    public static TimeValue extractTimeInterval(String input) {
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            int value = Integer.parseInt(matcher.group(1));
            String unit = matcher.group(3);
            if (unit == null || unit.isEmpty()) {
                return new TimeValue(value, TimeUnit.SECONDS);
            } else {
                switch (unit.toLowerCase()) {
                    case "h":
                        return new TimeValue(value, TimeUnit.HOURS);
                    case "m":
                        return new TimeValue(value, TimeUnit.MINUTES);
                    case "s":
                        return new TimeValue(value, TimeUnit.SECONDS);
                    default:
                        throw new IllegalArgumentException("Invalid time unit: " + unit);
                }
            }
        } else {
            throw new IllegalArgumentException("Invalid time value format: " + input);
        }
    }

    @Getter
    public static class TimeValue {
        private final int value;
        private final TimeUnit unit;

        public TimeValue(int value, TimeUnit unit) {
            this.value = value;
            this.unit = unit;
        }

    }

}