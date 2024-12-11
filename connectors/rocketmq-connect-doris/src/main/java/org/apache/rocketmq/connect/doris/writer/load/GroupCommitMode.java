package org.apache.rocketmq.connect.doris.writer.load;

import java.util.Arrays;
import java.util.List;

public enum GroupCommitMode {
    OFF_MODE("off_mode"),
    SYNC_MODE("sync_mode"),
    ASYNC_MODE("async_mode");

    private final String name;

    GroupCommitMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static LoadModel of(String name) {
        return LoadModel.valueOf(name.toUpperCase());
    }

    public static List<String> instances() {
        return Arrays.asList(OFF_MODE.name, SYNC_MODE.name, ASYNC_MODE.name);
    }
}
