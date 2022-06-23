package org.apache.rocketmq.connect.fc.sink;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.connect.fc.sink.constant.FcConstant;
import com.aliyuncs.fc.client.FunctionComputeClient;
import com.aliyuncs.fc.constants.Const;
import com.aliyuncs.fc.exceptions.ClientException;
import com.aliyuncs.fc.request.GetFunctionRequest;
import com.aliyuncs.fc.request.GetServiceRequest;
import com.aliyuncs.fc.request.InvokeFunctionRequest;
import com.aliyuncs.fc.response.InvokeFunctionResponse;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class FcSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(FcSinkTask.class);

    private String regionId;

    private String accessKeyId;

    private String accessKeySecret;

    private String accountId;

    private String serviceName;

    private String functionName;

    private String invocationType;

    private String qualifier;

    private FunctionComputeClient functionComputeClient;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(connectRecord -> {
                InvokeFunctionRequest invokeFunctionRequest = new InvokeFunctionRequest(serviceName, functionName);
                invokeFunctionRequest.setPayload(JSON.toJSONString(connectRecord.getData()).getBytes(StandardCharsets.UTF_8));
                if (!StringUtils.isBlank(invocationType)) {
                    invokeFunctionRequest.setInvocationType(Const.INVOCATION_TYPE_ASYNC);
                }
                invokeFunctionRequest.setQualifier(qualifier);
                InvokeFunctionResponse invokeFunctionResponse = functionComputeClient.invokeFunction(invokeFunctionRequest);
                if (Const.INVOCATION_TYPE_ASYNC.equals(invocationType)) {
                    if (HttpURLConnection.HTTP_ACCEPTED == invokeFunctionResponse.getStatus()) {
                        log.info("Async invocation has been queued for execution, request ID: {}", invokeFunctionResponse.getRequestId());
                    }else {
                        log.info("Async invocation was not accepted");
                    }
                }
            });
        } catch (Exception e) {
            log.error("FcSinkTask | put | error => ", e);
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void validate(KeyValue config) {
        try {
            GetServiceRequest getServiceRequest = new GetServiceRequest(config.getString(FcConstant.SERVICE_NAME_CONSTANT));
            getServiceRequest.setQualifier(config.getString(FcConstant.QUALIFIER_CONSTANT));
            functionComputeClient.getService(getServiceRequest);
            GetFunctionRequest getFunctionRequest = new GetFunctionRequest(config.getString(FcConstant.SERVICE_NAME_CONSTANT), config.getString(FcConstant.FUNCTION_NAME_CONSTANT));
            getFunctionRequest.setQualifier(config.getString(FcConstant.QUALIFIER_CONSTANT));
            functionComputeClient.getFunction(getFunctionRequest);
        } catch (ClientException e) {
            log.error("FcSinkTask | validate | error => ", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void init(KeyValue config) {
        regionId = config.getString(FcConstant.REGION_ID_CONSTANT);
        accessKeyId = config.getString(FcConstant.ACCESS_KEY_ID_CONSTANT);
        accessKeySecret = config.getString(FcConstant.ACCESS__KEY_SECRET_CONSTANT);
        accountId = config.getString(FcConstant.ACCOUNT_ID_CONSTANT);
        serviceName = config.getString(FcConstant.SERVICE_NAME_CONSTANT);
        functionName = config.getString(FcConstant.FUNCTION_NAME_CONSTANT);
        invocationType = config.getString(FcConstant.INVOCATION_TYPE_CONSTANT, null);
        qualifier = config.getString(FcConstant.QUALIFIER_CONSTANT, FcConstant.DEFAULT_QUALIFIER_CONSTANT);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        try {
            super.start(sinkTaskContext);
            functionComputeClient = new FunctionComputeClient(regionId, accountId, accessKeyId, accessKeySecret);
        } catch (Exception e) {
            log.error("FcSinkTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        functionComputeClient = null;
    }
}
