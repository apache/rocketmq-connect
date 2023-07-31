package org.apache.rocketmq.connect.hbase.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;
import junit.framework.TestCase;
import org.apache.rocketmq.connect.hbase.config.HBaseConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseSinkTaskTest extends TestCase {

    private static final String zkHost = "localhost:2181";
    private static final String zkPort = "2181";
    private static final String hbaseMaster = "localhost:16000";
    private static final String columnFamily = "cf";

    public static void main(String[] args) {
        List<ConnectRecord> records = new ArrayList<>();
        // build schema
        Schema schema = SchemaBuilder.struct()
                .name("tableName")
                .field("c1",SchemaBuilder.string().build())
                .field("c2", SchemaBuilder.string().build())
                .build();
        // build record
        String param0 = "1001";
        Struct struct= new Struct(schema);
        struct.put("c1",param0);
        struct.put("c2",String.format("test-data-%s", param0));

        Schema schema2 = SchemaBuilder.struct()
            .name("t1")
            .field("c1",SchemaBuilder.string().build())
            .field("c2", SchemaBuilder.string().build())
            .build();
        // build record
        Struct struct2= new Struct(schema2);
        struct.put("c1",param0);
        struct.put("c2",String.format("test-data-%s", param0));

        for (int i = 0; i < 4; i++) {
            ConnectRecord record = new ConnectRecord(
                // offset partition
                // offset partition"
                new RecordPartition(new ConcurrentHashMap<>()),
                new RecordOffset(new HashMap<>()),
                System.currentTimeMillis(),
                schema,
                struct
            );
            records.add(record);

            ConnectRecord record2 = new ConnectRecord(
                // offset partition
                // offset partition"
                new RecordPartition(new ConcurrentHashMap<>()),
                new RecordOffset(new HashMap<>()),
                System.currentTimeMillis(),
                schema2,
                struct
            );
            records.add(record2);

        }

        HBaseSinkTask task = new HBaseSinkTask();
        KeyValue config = new DefaultKeyValue();
        config.put(HBaseConstants.COLUMN_FAMILY, columnFamily);
        config.put(HBaseConstants.HBASE_MASTER_CONFIG, hbaseMaster);
        config.put(HBaseConstants.HBASE_ZK_HOST, zkHost);
        config.put(HBaseConstants.HBASE_ZK_PORT, zkPort);

        task.start(config);
        task.put(records);
    }
}