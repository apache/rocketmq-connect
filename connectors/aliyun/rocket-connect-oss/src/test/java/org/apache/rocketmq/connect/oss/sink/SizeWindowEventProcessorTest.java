package org.apache.rocketmq.connect.oss.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.oss.config.TaskConfig;
import org.apache.rocketmq.connect.oss.util.StringGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SizeWindowEventProcessorTest {
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
        // 测试文件写入大小[256KB,512KB,1024KB,2048KB],batchSize=4MB
        List<Integer> testDataSize = new ArrayList<>();
        testDataSize.add(256);
        testDataSize.add(512);
        testDataSize.add(1024);
        testDataSize.add(2048);
        // 初始化sink task任务并启动
        OSSSinkTask task = new OSSSinkTask();
        KeyValue config = new DefaultKeyValue();
        config.put(TaskConfig.ENDPOINT, endpoint);
        config.put(TaskConfig.ACCESS_KEY_ID, accessKeyId);
        config.put(TaskConfig.ACCESS_KEY_SECRET, accessKeySecret);
        config.put(TaskConfig.BUCKET_NAME, bucketName);
        config.put(TaskConfig.BATCH_SEND_TYPE, "size");
        config.put(TaskConfig.BATCH_SEND_GROUP_SIZE, "4MB");
        task.start(config);
        for (Integer size : testDataSize) {
            for (int i = 0; i < 16; i++) {
                ConnectRecord record = new ConnectRecord(
                        new RecordPartition(new ConcurrentHashMap<>()),
                        new RecordOffset(new HashMap<>()),
                        System.currentTimeMillis(),
                        keySchema,
                        "SizeWindowEventProcessorTest/group1_" + i + "_size_" + size + "KB",
                        valSchema,
                        StringGenerator.generate(size));
                task.put(Collections.singletonList(record));
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
