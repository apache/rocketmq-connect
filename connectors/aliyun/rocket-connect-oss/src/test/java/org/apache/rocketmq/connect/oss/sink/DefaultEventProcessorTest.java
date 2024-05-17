package org.apache.rocketmq.connect.oss.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.oss.config.TaskConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultEventProcessorTest {
     private static final String endpoint = "https://oss-cn-beijing.aliyuncs.com";
     private static final String bucketName = "xxxxx";
     private static final String accessKeyId = "xxxxxxxxxxxxx";
     private static final String accessKeySecret = "xxxxxxxxxxxxxxxxx";

     public static void main(String[] args) {
         List<ConnectRecord> records = new ArrayList<>();
         // build schema
         Schema keySchema = SchemaBuilder.string()
                 .build();

         Schema valSchema = SchemaBuilder.string()
                 .build();

         for (int i = 0; i < 4; i++) {

             ConnectRecord record = new ConnectRecord(
                     new RecordPartition(new ConcurrentHashMap<>()),
                     new RecordOffset(new HashMap<>()),
                     System.currentTimeMillis(),
                     keySchema,
                     "DefaultEventProcessorTest/test_" + i,
                     valSchema,
                     "DefaultEventProcessor send data success,msg:" + i);
             records.add(record);

         }

         OSSSinkTask task = new OSSSinkTask();
         KeyValue config = new DefaultKeyValue();
         config.put(TaskConfig.ENDPOINT, endpoint);
         config.put(TaskConfig.ACCESS_KEY_ID, accessKeyId);
         config.put(TaskConfig.ACCESS_KEY_SECRET, accessKeySecret);
         config.put(TaskConfig.BUCKET_NAME, bucketName);
         task.start(config);
         task.put(records);
     }
}
