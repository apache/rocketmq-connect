package org.apache.rocketmq.connect.clickhouse.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.List;
import junit.framework.TestCase;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseConstants;

import static java.lang.Thread.sleep;

public class ClickHouseSourceTaskTest extends TestCase {

    private static final String host = "120.48.26.195";
    private static final String port = "8123";
    private static final String db = "default";
    private static final String username = "default";
    private static final String password = "123456";

    public void testPoll() {
    }

    public void testStart() throws InterruptedException {
        ClickHouseSourceTask task = new ClickHouseSourceTask();
        KeyValue config = new DefaultKeyValue();
        config.put(ClickHouseConstants.CLICKHOUSE_HOST, host);
        config.put(ClickHouseConstants.CLICKHOUSE_PORT, port);
        config.put(ClickHouseConstants.CLICKHOUSE_DATABASE, db);
        config.put(ClickHouseConstants.CLICKHOUSE_USERNAME, username);
        config.put(ClickHouseConstants.CLICKHOUSE_PASSWORD, password);
        config.put(ClickHouseConstants.CLICKHOUSE_TABLE, "tableName");
        task.start(config);
        while (true) {
            List<ConnectRecord> records = task.poll();
            for (ConnectRecord r : records) {
                System.out.println(r);
            }
            sleep(3000);
        }
    }
}