package org.apache.rocketmq.connect.jdbc.connector.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.jdbc.connector.JdbcSinkTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class ClickHouseJdbcSinkTest {

    private static JdbcSinkTask CKHouseSinkTask = new JdbcSinkTask();
    private static final String dbName = "default";
    private static final String tableName = "tb_test";
    private static final String ip = "47.118.70.66";
    private static final String port = "18123";
    @Before
    public void testCKHouseSinkConfig() {
        KeyValue config = buildClickHouseSinkConfig();
        CKHouseSinkTask.start(config);
    }

    @After
    public void close() {}


    private KeyValue buildClickHouseSinkConfig() {
        KeyValue conf = new DefaultKeyValue();
        conf.put("connector-class", "org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConnector");
        conf.put("max-task", "1");
        conf.put("connect-topicname", "tb_test");
        conf.put("connection.url",String.format("jdbc:clickhouse://%s:%s",ip,port));
        conf.put("connection.user", "");
        conf.put("connection.password","");
        conf.put("pk.fields", "id");
        conf.put("pk.mode" , "record_value");
        conf.put("insert.mode" ,"UPSERT");
        conf.put("source-record-converter", "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter");
        return conf;
    }

    /**
     * sink writer test
     */
    @Test
    public void testOpenMLDBJdbcSinkWriterTest() throws SQLException {
        List<ConnectRecord> records = new ArrayList<>();

        // build schema
        Schema schema = SchemaBuilder.struct()
                .name(tableName)
                .field("id",SchemaBuilder.int32().build())
                .field("name", SchemaBuilder.string().build())
                .build();
      
        // build record
        int id = 1;
        String name = "test";
        io.openmessaging.connector.api.data.Struct struct= new Struct(schema);
        struct.put("id",id);
        struct.put("name",name);

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
        CKHouseSinkTask.put(records);
    }

    @Test
    public void test1() {
        List<Integer> integers = Arrays.asList(1, 2, 3, 4);
        char c = '5';
        int a = c - '0';
        System.out.println(a);
    }
}
