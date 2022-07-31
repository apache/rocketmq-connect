package org.apache.rocketmq.connect.eventbridge.sink;

import com.aliyun.eventbridge.EventBridgeClient;
import com.aliyun.eventbridge.models.CloudEvent;
import com.aliyun.eventbridge.models.Config;
import com.aliyun.eventbridge.models.PutEventsResponse;
import com.aliyun.eventbridge.util.EventBuilder;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithServiceIdentityRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithServiceIdentityResponse;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.eventbridge.sink.constant.EventBridgeConstant;
import org.apache.rocketmq.connect.eventbridge.sink.utils.CheckUtils;
import org.apache.rocketmq.connect.eventbridge.sink.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EventBridgeSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventBridgeSinkTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String stsEndpoint;

    private String roleArn;

    private String roleSessionName;

    private String eventSubject;

    private String aliyuneventbusname;

    private String accountEndpoint;

    private EventBridgeClient eventBridgeClient;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        List<CloudEvent> cloudEventList = new ArrayList<>();
        try {
            sinkRecords.forEach(connectRecord -> cloudEventList.add(EventBuilder.builder()
                    .withId(connectRecord.getExtension(EventBridgeConstant.EVENT_ID))
                    .withSource(URI.create(connectRecord.getExtension(EventBridgeConstant.EVENT_SOURCE)))
                    .withType(connectRecord.getExtension(EventBridgeConstant.EVENT_TYPE))
                    .withSubject(eventSubject)
                    .withTime(CheckUtils.checkNull(connectRecord.getExtension(EventBridgeConstant.EVENT_TIME)) ? new Date() : DateUtils.getDate(connectRecord.getExtension(EventBridgeConstant.EVENT_TIME), DateUtils.DEFAULT_DATE_FORMAT))
                    .withJsonStringData(connectRecord.getData().toString())
                    .withAliyunEventBus(aliyuneventbusname)
                    .build()));
            PutEventsResponse putEventsResponse = eventBridgeClient.putEvents(cloudEventList);
            log.info("EventBridgeSinkTask | put | putEventsResponse | eventId : {} | traceId : {} | requestId : {}", putEventsResponse.getEntryList().get(0).getEventId(), putEventsResponse.getEntryList().get(0).getTraceId(), putEventsResponse.getRequestId());
        } catch (Exception e) {
            log.error("EventBridgeSinkTask | put | error => ", e);
            throw new RuntimeException(e);
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

    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(EventBridgeConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(EventBridgeConstant.ACCESS_KEY_SECRET);
        roleArn = config.getString(EventBridgeConstant.ROLE_ARN);
        roleSessionName = config.getString(EventBridgeConstant.ROLE_SESSION_NAME);
        eventSubject = config.getString(EventBridgeConstant.EVENT_SUBJECT);
        aliyuneventbusname = config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME);
        accountEndpoint = config.getString(EventBridgeConstant.ACCOUNT_ENDPOINT);
        stsEndpoint = config.getString(EventBridgeConstant.STS_ENDPOINT);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
        try {
            Config authConfig = new Config();
            if (CheckUtils.checkNotNull(roleArn) && CheckUtils.checkNotNull(roleSessionName)) {
                DefaultProfile.addEndpoint("", "", "Sts", stsEndpoint);
                DefaultProfile profile = DefaultProfile.getProfile("", accessKeyId, accessKeySecret);
                IAcsClient client = new DefaultAcsClient(profile);
                AssumeRoleWithServiceIdentityRequest request = new AssumeRoleWithServiceIdentityRequest();
                request.setRoleArn(roleArn);
                request.setRoleSessionName(roleSessionName);
                request.setAssumeRoleFor(roleSessionName);
                request.setAcceptFormat(FormatType.JSON);
                request.setDurationSeconds(3600L);
                final AssumeRoleWithServiceIdentityResponse response = client.getAcsResponse(request);
                authConfig.accessKeyId = response.getCredentials().getAccessKeyId();
                authConfig.accessKeySecret = response.getCredentials().getAccessKeySecret();
                authConfig.securityToken = response.getCredentials().getSecurityToken();
            } else {
                authConfig.accessKeyId = accessKeyId;
                authConfig.accessKeySecret = accessKeySecret;
            }
            authConfig.endpoint = accountEndpoint;
            eventBridgeClient = new EventBridgeClient(authConfig);
        } catch (Exception e) {
            log.error("EventBridgeSinkTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        eventBridgeClient = null;
    }

}
