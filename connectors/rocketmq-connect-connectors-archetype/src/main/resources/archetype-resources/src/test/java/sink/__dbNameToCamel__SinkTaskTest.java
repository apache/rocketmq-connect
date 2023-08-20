#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


class ${dbNameToCamel}SinkTaskTest {
//
//    private static final String host = "127.0.0.1";
//    private static final String port = "8123";
//    private static final String db = "default";
//    private static final String username = "default";
//    private static final String password = "123456";
//
//
//
//    public static void main(String[] args) {
//        List<ConnectRecord> records = new ArrayList<>();
//        // build schema
//        Schema schema = SchemaBuilder.struct()
//                .name("tableName")
//                .field("c1",SchemaBuilder.string().build())
//                .field("c2", SchemaBuilder.string().build())
//                .build();
//        // build record
//        String param0 = "1001";
//        Struct struct= new Struct(schema);
//        struct.put("c1",param0);
//        struct.put("c2",String.format("test-data-%s", param0));
//
//        Schema schema2 = SchemaBuilder.struct()
//            .name("t1")
//            .field("c1",SchemaBuilder.string().build())
//            .field("c2", SchemaBuilder.string().build())
//            .build();
//        // build record
//        Struct struct2= new Struct(schema2);
//        struct.put("c1",param0);
//        struct.put("c2",String.format("test-data-%s", param0));
//
//        for (int i = 0; i < 4; i++) {
//            ConnectRecord record = new ConnectRecord(
//                // offset partition
//                // offset partition"
//                new RecordPartition(new ConcurrentHashMap<>()),
//                new RecordOffset(new HashMap<>()),
//                System.currentTimeMillis(),
//                schema,
//                struct
//            );
//            records.add(record);
//
//            ConnectRecord record2 = new ConnectRecord(
//                // offset partition
//                // offset partition"
//                new RecordPartition(new ConcurrentHashMap<>()),
//                new RecordOffset(new HashMap<>()),
//                System.currentTimeMillis(),
//                schema2,
//                struct
//            );
//            records.add(record2);
//
//        }
//
//        ${dbNameToCamel}SinkTask task = new ${dbNameToCamel}SinkTask();
//        KeyValue config = new DefaultKeyValue();
//        task.start(config);
//        task.put(records);
//
//    }

}