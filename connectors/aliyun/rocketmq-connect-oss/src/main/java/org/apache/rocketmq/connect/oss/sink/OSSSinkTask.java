package org.apache.rocketmq.connect.oss.sink;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.oss.config.OSSBaseConfig;
import org.apache.rocketmq.connect.oss.config.OSSConstants;
import org.apache.rocketmq.connect.oss.helper.OSSHelperClient;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;

public class OSSSinkTask extends SinkTask {

    private OSSBaseConfig config;
    private OSSHelperClient ossClient;

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(OSSConstants.ENDPOINT))
                || StringUtils.isBlank(config.getString(OSSConstants.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(OSSConstants.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(OSSConstants.BUCKET_NAME))) {
            throw new RuntimeException("OSS required parameter is null !");
        }

        if (ossClient.isBucketExist(OSSConstants.BUCKET_NAME)) {
            throw new RuntimeException("Bucket is not exist !");
        }
    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = new OSSBaseConfig();
        this.config.load(keyValue);
        this.ossClient = new OSSHelperClient(this.config);
    }

    @Override
    public void stop() {
        this.ossClient.stop();
        this.ossClient = null;
    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }

        for (ConnectRecord record : sinkRecords) {
            this.ossClient.putObject(config.getBucketName(), (String) record.getKey(),
                    record.getData().toString());
        }
    }

}
