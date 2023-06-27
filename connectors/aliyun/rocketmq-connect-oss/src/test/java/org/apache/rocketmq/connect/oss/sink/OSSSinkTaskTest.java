package org.apache.rocketmq.connect.oss.sink;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;

import io.openmessaging.internal.DefaultKeyValue;

import org.apache.rocketmq.connect.oss.config.OSSConstants;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class OSSSinkTaskTest {
    // private static final String endpoint = "https://oss-cn-hangzhou.aliyuncs.com";
    // private static final String bucketName = "ExampleBucket";
    // private static final String accessKeyId = "<accessKeyId>";
    // private static final String accessKeySecret = "<accessKeySecret>";

    // public static void main(String[] args) {
    //     List<ConnectRecord> records = new ArrayList<>();
    //     // build schema
    //     Schema keySchema = SchemaBuilder.string()
    //             .build();

    //     Schema valSchema = SchemaBuilder.string()
    //             .build();

    //     for (int i = 0; i < 4; i++) {

    //         ConnectRecord record = new ConnectRecord(
    //                 new RecordPartition(new ConcurrentHashMap<>()),
    //                 new RecordOffset(new HashMap<>()),
    //                 System.currentTimeMillis(),
    //                 keySchema,
    //                 "key" + i,
    //                 valSchema,
    //                 "data" + i);
    //         records.add(record);

    //     }

    //     OSSSinkTask task = new OSSSinkTask();
    //     KeyValue config = new DefaultKeyValue();
    //     config.put(OSSConstants.ENDPOINT, endpoint);
    //     config.put(OSSConstants.ACCESS_KEY_ID, accessKeyId);
    //     config.put(OSSConstants.ACCESS_KEY_SECRET, accessKeySecret);
    //     config.put(OSSConstants.BUCKET_NAME, bucketName);
    //     task.start(config);
    //     task.put(records);
    // }
}
