package org.apache.rocketmq.connect.oss.helper;

import java.io.ByteArrayInputStream;

import org.apache.rocketmq.connect.oss.config.OSSBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyuncs.exceptions.ClientException;

public class OSSHelperClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(OSSHelperClient.class);

    private OSSBaseConfig config;
    OSS ossClient;

    public OSSHelperClient(OSSBaseConfig config) {
        this.config = config;
        this.ossClient = create(this.config);
    }

    private OSS create(OSSBaseConfig config) {
        if (config.getAccessKeyId() != null && config.getAccessKeySecret() != null) {
            return new OSSClientBuilder().build(config.getEndpoint(), config.getAccessKeyId(),
                    config.getAccessKeySecret());
        }

        try {
            EnvironmentVariableCredentialsProvider credentialsProvider = CredentialsProviderFactory
                    .newEnvironmentVariableCredentialsProvider();
            return new OSSClientBuilder().build(config.getEndpoint(), credentialsProvider);
        } catch (ClientException e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Credentials cannot be empty!");
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
        ossClient.putObject(putObjectRequest);
    }

    public void stop() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }

}
