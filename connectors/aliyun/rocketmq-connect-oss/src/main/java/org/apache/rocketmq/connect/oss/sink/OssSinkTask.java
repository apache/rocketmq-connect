package org.apache.rocketmq.connect.oss.sink;


import com.aliyuncs.DefaultAcsClient;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.profile.DefaultProfile;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.rocketmq.connect.oss.sink.constant.OssConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyun.oss.common.auth.*;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;

import java.util.stream.Collectors;
import java.util.HashMap; 
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;

public class OssSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(OssSinkTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String bucketName;

    private String fileUrlPrefix;

    private String region;

    private OSS ossClient;

    private String objectName;

    private String partitionMethod;

    private String compressType;

    private boolean enableBatchPut;

    private String taskId;

    private long lastOffset;

    private long lastTimeStamp;

    private String lastPrefix;

    private HashMap<String, List<String>> recordMap = new HashMap<>();

    private void processMap() throws ConnectException, IOException {
        recordMap.forEach((key, values) -> {  
            String joinedString = values.stream().collect(Collectors.joining("\n"));
            String absolutePath = key + objectName;
            boolean exists = ossClient.doesObjectExist(bucketName, absolutePath);
            long offset = 0;
            // If the object does not exist, create it and set offset to 0, otherwise read the offset of the current object
            if (exists) {
                try {
                    OSSObject ossObject = ossClient.getObject(bucketName, absolutePath);
                    InputStream inputStream = ossObject.getObjectContent();
                    offset = inputStream.available();
                } catch (Exception e) {
                    log.error("OSSSinkTask | getObjectContent | error => ", e);
                }
            } else {
                offset = 0;
            }
            putOss(absolutePath, offset, joinedString);
        });
    }

    private String genFilePrefixByPartition(ConnectRecord record) throws ConnectException {
        if (partitionMethod.equals("Normal")) {
            return fileUrlPrefix;
        } else if (partitionMethod.equals("Time")) {
            long nowTimeStamp = record.getTimestamp();
            if (lastTimeStamp != nowTimeStamp) {
                Date myDate = new Date(nowTimeStamp);
                String year = String.format("%tY", myDate);
                String month = String.format("%tm", myDate);
                String day = String.format("%td", myDate);
                String hour = String.format("%tH", myDate);
                lastPrefix = fileUrlPrefix + year + "/" + month + "/" + day + "/" + hour + "/";
                return lastPrefix;
            }
            return lastPrefix;
        } else {
            throw new RetriableException("Illegal partition method.");
        }
    }

    private long genObjectOffset(ConnectRecord record, String objectUrl) throws ConnectException, IOException {
        if (partitionMethod.equals("Normal")) {
            return lastOffset;
        } else if (partitionMethod.equals("Time")) {
            if (lastTimeStamp != record.getTimestamp()) {
                boolean exists = ossClient.doesObjectExist(bucketName, objectUrl);
                // If the object does not exist, create it and set offset to 0, otherwise read the offset of the current object
                if (exists) {
                    OSSObject ossObject = ossClient.getObject(bucketName, objectUrl);
                    InputStream inputStream = ossObject.getObjectContent();
                    lastOffset = inputStream.available();
                    return lastOffset;
                } else {
                    lastOffset = 0;
                    return lastOffset;
                }
            } else {
                return lastOffset;
            }
        } else {
            throw new RetriableException("Illegal partition method.");
        }
    }

    private void putOss(String absolutePath, long offset, String context) throws ConnectException {
        try {
            // Create an append write request and send it
            AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, absolutePath, new ByteArrayInputStream(context.getBytes()));
            appendObjectRequest.setPosition(offset);
            AppendObjectResult appendObjectResult = ossClient.appendObject(appendObjectRequest);

            // Update
            lastOffset = appendObjectResult.getNextPosition();
        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means your request made it to OSS, "
                + "but was rejected with an error response for some reason.");
            System.out.println("Error Message:" + oe.getErrorMessage());
            System.out.println("Error Code:" + oe.getErrorCode());
            System.out.println("Request ID:" + oe.getRequestId());
            System.out.println("Host ID:" + oe.getHostId());
        } catch (ClientException ce) {
            System.out.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message:" + ce.getMessage());
        }
    }

    private void handleRecord(ConnectRecord record) throws ConnectException, IOException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", record.getData());
        String context = JSON.toJSONString(jsonObject);
        String prefix = genFilePrefixByPartition(record);
        if (enableBatchPut) {
            if (!recordMap.containsKey(prefix)) {
                recordMap.put(prefix, new ArrayList<>()); 
            }
            recordMap.get(prefix).add(context);
        } else {
            String absolutePath = prefix + objectName;
            long appendOffset = genObjectOffset(record, absolutePath);
            putOss(absolutePath, appendOffset, context);
            lastTimeStamp = record.getTimestamp();
        }
    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(sinkRecord -> {
                try {
                    handleRecord(sinkRecord);
                } catch (OSSException oe) {
                    System.out.println("Caught an OSSException, which means your request made it to OSS, "
                            + "but was rejected with an error response for some reason.");
                    System.out.println("Error Message:" + oe.getErrorMessage());
                    System.out.println("Error Code:" + oe.getErrorCode());
                    System.out.println("Request ID:" + oe.getRequestId());
                    System.out.println("Host ID:" + oe.getHostId());
                } catch (ClientException ce) {
                    System.out.println("Caught an ClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with OSS, "
                            + "such as not being able to access the network.");
                    System.out.println("Error Message:" + ce.getMessage());
                } catch (Exception e) {
                    log.error("OSSSinkTask | genObjectOffset | error => ", e);
                }
            });
            if (enableBatchPut && !recordMap.isEmpty()) {  
                processMap();
            }
        } catch (Exception e) {
            log.error("OSSSinkTask | put | error => ", e);
        }
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void init(SinkTaskContext sinkTaskContext) {
        taskId = sinkTaskContext.getConnectorName() + "-" +  sinkTaskContext.getTaskName();
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
        compressType = config.getString(OssConstant.COMPRESS_TYPE);
        enableBatchPut = Boolean.parseBoolean(config.getString(OssConstant.ENABLE_BATCH_PUT, "false"));
        
        fileUrlPrefix = fileUrlPrefix + taskId + "/";
        try {
            DefaultCredentialProvider credentialsProvider = CredentialsProviderFactory.newDefaultCredentialProvider(accessKeyId, accessKeySecret);

            ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
            clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
            ossClient = OSSClientBuilder.create()
                    .endpoint(accountEndpoint)
                    .credentialsProvider(credentialsProvider)
                    .clientConfiguration(clientBuilderConfiguration)
                    .region(region)
                    .build();
            if (partitionMethod.equals("Normal")) {
                boolean exists = ossClient.doesObjectExist(bucketName, fileUrlPrefix + objectName);
                // If the object does not exist, create it and set offset to 0, otherwise read the offset of the current object
                if (exists) {
                    OSSObject ossObject = ossClient.getObject(bucketName, fileUrlPrefix + objectName);
                    InputStream inputStream = ossObject.getObjectContent();
                    long offset_now = inputStream.available();
                    lastOffset = offset_now;
                } else {
                    lastOffset = 0;
                }
            }
        } catch (Exception e) {
            log.error("OssSinkTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        ossClient.shutdown();
    }

}
