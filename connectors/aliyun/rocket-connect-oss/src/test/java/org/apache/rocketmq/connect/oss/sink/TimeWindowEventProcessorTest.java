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

public class TimeWindowEventProcessorTest {
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

        // 初始化sink task任务并启动
        OSSSinkTask task = new OSSSinkTask();
        KeyValue config = new DefaultKeyValue();
        config.put(TaskConfig.ENDPOINT, endpoint);
        config.put(TaskConfig.ACCESS_KEY_ID, accessKeyId);
        config.put(TaskConfig.ACCESS_KEY_SECRET, accessKeySecret);
        config.put(TaskConfig.BUCKET_NAME, bucketName);
        config.put(TaskConfig.BATCH_SEND_TYPE, "time");
        config.put(TaskConfig.BATCH_SEND_TIME_INTERVAL, "30s");
        task.start(config);
        for (int i = 0; i < 30; i++) {
            ConnectRecord record = new ConnectRecord(
                    new RecordPartition(new ConcurrentHashMap<>()),
                    new RecordOffset(new HashMap<>()),
                    System.currentTimeMillis(),
                    keySchema,
                    "TimeWindowEventProcessorTest/test_" + i + "_size_1KB",
                    valSchema,
                    StringGenerator.generate(1));
            task.put(Collections.singletonList(record));
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
