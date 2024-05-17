package org.apache.rocketmq.connect.oss.util;

import java.util.Random;

public class StringGenerator {
    private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

    public static String generate(int sizeInKB) {
        int targetSize = sizeInKB * 1024;
        StringBuilder sb = new StringBuilder(targetSize);
        Random random = new Random();

        for (int i = 0; i < targetSize; i++) {
            sb.append(CHARS[random.nextInt(CHARS.length)]);
        }

        return sb.toString();
    }
}
