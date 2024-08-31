package org.apache.rocketmq.connect.oss.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.oss.sink.constant.OssConstant;

import java.util.ArrayList;
import java.util.List;

public class OssSinkConnector extends SinkConnector {

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String bucketName;

    private String fileUrlPrefix;

    private String objectName;

    private String region;

    private String partitionMethod;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(OssConstant.ACCESS_KEY_ID, accessKeyId);
        keyValue.put(OssConstant.ACCESS_KEY_SECRET, accessKeySecret);
        keyValue.put(OssConstant.ACCOUNT_ENDPOINT, accountEndpoint);
        keyValue.put(OssConstant.BUCKET_NAME, bucketName);
        keyValue.put(OssConstant.FILE_URL_PREFIX, fileUrlPrefix);
        keyValue.put(OssConstant.OBJECT_NAME, objectName);
        keyValue.put(OssConstant.REGION, region);
        keyValue.put(OssConstant.PARTITION_METHOD, partitionMethod);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OssSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(OssConstant.ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(OssConstant.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(OssConstant.ACCOUNT_ENDPOINT))
                || StringUtils.isBlank(config.getString(OssConstant.BUCKET_NAME))
                || StringUtils.isBlank(config.getString(OssConstant.OBJECT_NAME))
                || StringUtils.isBlank(config.getString(OssConstant.REGION))
                || StringUtils.isBlank(config.getString(OssConstant.PARTITION_METHOD))) {
            throw new RuntimeException("Oss required parameter is null !");
        }
    }

    @Override
    public void start(KeyValue config) {
        accessKeyId = config.getString(OssConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(OssConstant.ACCESS_KEY_SECRET);
        accountEndpoint = config.getString(OssConstant.ACCOUNT_ENDPOINT);
        bucketName = config.getString(OssConstant.BUCKET_NAME);
        fileUrlPrefix = config.getString(OssConstant.FILE_URL_PREFIX);
        objectName = config.getString(OssConstant.OBJECT_NAME);
        region = config.getString(OssConstant.REGION);
        partitionMethod = config.getString(OssConstant.PARTITION_METHOD);
    }

    @Override
    public void stop() {

    }
}
