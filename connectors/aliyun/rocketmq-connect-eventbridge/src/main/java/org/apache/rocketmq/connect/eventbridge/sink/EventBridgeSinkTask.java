package org.apache.rocketmq.connect.eventbridge.sink;

import com.aliyun.eventbridge.EventBridgeClient;
import com.aliyun.eventbridge.models.CloudEvent;
import com.aliyun.eventbridge.models.Config;
import com.aliyun.eventbridge.models.PutEventsResponse;
import com.aliyun.eventbridge.util.EventBuilder;
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

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String eventId;

    private String eventSource;

    private String eventTime;

    private String eventType;

    private String eventSubject;

    private String aliyuneventbusname;

    private EventBridgeClient eventBridgeClient;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        List<CloudEvent> cloudEventList = new ArrayList<>();
        try {
            sinkRecords.forEach(connectRecord -> cloudEventList.add(EventBuilder.builder()
                    .withId(eventId)
                    .withSource(URI.create(eventSource))
                    .withType(eventType)
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
        accountEndpoint = config.getString(EventBridgeConstant.ACCOUNT_ENDPOINT);
        eventId = config.getString(EventBridgeConstant.EVENT_ID);
        eventSource = config.getString(EventBridgeConstant.EVENT_SOURCE);
        eventTime = config.getString(EventBridgeConstant.EVENT_TIME);
        eventType = config.getString(EventBridgeConstant.EVENT_TYPE);
        eventSubject = config.getString(EventBridgeConstant.EVENT_SUBJECT);
        aliyuneventbusname = config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
        try {
            Config authConfig = new Config();
            authConfig.accessKeyId = accessKeyId;
            authConfig.accessKeySecret = accessKeySecret;
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
