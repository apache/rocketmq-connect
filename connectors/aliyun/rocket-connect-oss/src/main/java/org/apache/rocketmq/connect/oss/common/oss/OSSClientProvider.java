package org.apache.rocketmq.connect.oss.common.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyuncs.exceptions.ClientException;
import org.apache.rocketmq.connect.oss.config.TaskConfig;
import org.apache.rocketmq.connect.oss.exception.OSSSinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

public class OSSClientProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(OSSClientProvider.class);

    private final TaskConfig config;
    private OSS ossClient;

    public OSSClientProvider(TaskConfig config) {
        this.config = config;
        try {
            if (config.getAccessKeyId() == null && config.getAccessKeySecret() == null
                    && config.getEndpoint() == null && config.getBucketName() == null) {
                throw new OSSSinkRuntimeException("OSSSinkConnector initialized failed, please check the oss client config if is correct");
            }
            ossClient = buildClient(this.config);
        } catch (OSSSinkRuntimeException e) {
            LOGGER.warn("OSSSinkConnector initialized failed, please check the oss client config if is correct");
        }
    }

    private OSS buildClient(TaskConfig config) throws OSSSinkRuntimeException {
        if (config.getAccessKeyId() != null && config.getAccessKeySecret() != null) {
            return new OSSClientBuilder().build(config.getEndpoint(), config.getAccessKeyId(),
                    config.getAccessKeySecret());
        } else {
            try {
                EnvironmentVariableCredentialsProvider credentialsProvider = CredentialsProviderFactory
                        .newEnvironmentVariableCredentialsProvider();
                return new OSSClientBuilder().build(config.getEndpoint(), credentialsProvider);
            } catch (ClientException e) {
                LOGGER.error("buildClient with error...{}",e.getErrorDescription());
                throw new OSSSinkRuntimeException(e);
            }

        }
    }

    public OSS getOSSClient() {
        return ossClient;
    }

    public boolean isBucketExist(String bucketName) {
        return ossClient.doesBucketExist(bucketName);
    }

    public void putObject(String bucketName, String objectName, String content) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName,
                new ByteArrayInputStream(content.getBytes()));
    }

    public void stop() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }
}
