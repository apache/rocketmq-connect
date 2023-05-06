package org.apache.rocketmq.connect.clickhouse.config;

import java.util.HashSet;
import java.util.Set;

public class ClickHouseSinkConfig extends ClickHouseBaseConfig {
    public static final Set<String> SINK_REQUEST_CONFIG = new HashSet<String>() {
        {
            add(ClickHouseConstants.CLICKHOUSE_HOST);
            add(ClickHouseConstants.CLICKHOUSE_PORT);
        }
    };
}
