package org.apache.rocketmq.connect.eventbridge.sink;

import com.aliyun.eventbridge.EventBridgeClient;
import com.aliyun.eventbridge.models.CloudEvent;
import com.aliyun.eventbridge.models.Config;
import com.aliyun.eventbridge.models.PutEventsResponse;
import com.aliyun.eventbridge.util.EventBuilder;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.eventbridge.sink.constant.EventBridgeConstant;
import org.apache.rocketmq.connect.eventbridge.sink.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class EventBridgeSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EventBridgeSinkTask.class);

    private String regionId;

    private String accessKeyId;

    private String accessKeySecret;

    private String roleArn;

    private String roleSessionName;

    private String eventTime;

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
                    .withTime(DateUtils.getDate(eventTime, DateUtils.DEFAULT_DATE_FORMAT))
                    .withJsonStringData(connectRecord.getData().toString())
                    .withAliyunEventBus(aliyuneventbusname)
                    .build()));
            PutEventsResponse putEventsResponse = eventBridgeClient.putEvents(cloudEventList);
            log.info("EventBridgeSinkTask | put | putEventsResponse | entryList : {} | requestId : {}", putEventsResponse.getEntryList(), putEventsResponse.getRequestId());
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
        eventTime = config.getString(EventBridgeConstant.EVENT_TIME);
        eventSubject = config.getString(EventBridgeConstant.EVENT_SUBJECT);
        aliyuneventbusname = config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME);
        regionId = config.getString(EventBridgeConstant.REGION_ID_CONSTANT);
        accountEndpoint = config.getString(EventBridgeConstant.ACCOUNT_ENDPOINT);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
        try {
            DefaultProfile profile = DefaultProfile.getProfile(regionId, accessKeyId, accessKeySecret);
            IAcsClient client = new DefaultAcsClient(profile);
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRegionId(regionId);
            request.setRoleArn(roleArn);
            request.setRoleSessionName(roleSessionName);
            AssumeRoleResponse response = client.getAcsResponse(request);
            Config authConfig = new Config();
            authConfig.accessKeyId = response.getCredentials().getAccessKeyId();
            authConfig.accessKeySecret = response.getCredentials().getAccessKeySecret();
            authConfig.securityToken = response.getCredentials().getSecurityToken();
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
