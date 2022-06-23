package org.apache.rocketmq.connect.fc.sink;

import org.apache.rocketmq.connect.fc.sink.constant.FcConstant;
import com.aliyuncs.fc.client.FunctionComputeClient;
import com.aliyuncs.fc.request.InvokeFunctionRequest;
import com.aliyuncs.fc.response.InvokeFunctionResponse;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class FcSinkTaskTest {

    @InjectMocks
    private final FcSinkTask fcSinkTask = new FcSinkTask();
    @Mock
    private FunctionComputeClient functionComputeClient;
    @Mock
    private SinkTaskContext sinkTaskContext;

    @Before
    public void before() {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(FcConstant.REGION_ID_CONSTANT, FcConstant.REGION_ID_CONSTANT);
        keyValue.put(FcConstant.ACCESS_KEY_ID_CONSTANT, FcConstant.ACCESS_KEY_ID_CONSTANT);
        keyValue.put(FcConstant.ACCESS__KEY_SECRET_CONSTANT, FcConstant.ACCESS__KEY_SECRET_CONSTANT);
        keyValue.put(FcConstant.ACCOUNT_ID_CONSTANT, FcConstant.ACCOUNT_ID_CONSTANT);
        keyValue.put(FcConstant.SERVICE_NAME_CONSTANT, FcConstant.SERVICE_NAME_CONSTANT);
        keyValue.put(FcConstant.FUNCTION_NAME_CONSTANT, FcConstant.FUNCTION_NAME_CONSTANT);
        keyValue.put(FcConstant.INVOCATION_TYPE_CONSTANT, FcConstant.INVOCATION_TYPE_CONSTANT);
        keyValue.put(FcConstant.QUALIFIER_CONSTANT, FcConstant.QUALIFIER_CONSTANT);
        fcSinkTask.init(keyValue);
    }

    @Test
    public void testStart() {
        sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        fcSinkTask.start(sinkTaskContext);
    }

    @Test
    public void testPut() {
        functionComputeClient = Mockito.mock(FunctionComputeClient.class);
        List<ConnectRecord> sinkRecords = new ArrayList<>(11);
        Map<String, String> partition = new HashMap<>();
        RecordPartition recordPartition = new RecordPartition(partition);
        RecordOffset recordOffset = new RecordOffset(partition);
        ConnectRecord connectRecord = new ConnectRecord(recordPartition, recordOffset, new Date().getTime());
        connectRecord.setData(new HashMap<String, String>());
        sinkRecords.add(connectRecord);
        InvokeFunctionResponse invokeFunctionResponse = new InvokeFunctionResponse();
        invokeFunctionResponse.setStatus(202);
        invokeFunctionResponse.setContent("content test".getBytes(StandardCharsets.UTF_8));
        Mockito.when(functionComputeClient.invokeFunction(any(InvokeFunctionRequest.class))).thenReturn(invokeFunctionResponse);
        fcSinkTask.put(sinkRecords);
    }

    @Test
    public void testFcPut() {
        FcSinkTask fcSinkTask = new FcSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(FcConstant.REGION_ID_CONSTANT, "cn-hangzhou");
        keyValue.put(FcConstant.ACCESS_KEY_ID_CONSTANT, "xxxx");
        keyValue.put(FcConstant.ACCESS__KEY_SECRET_CONSTANT, "xxxx");
        keyValue.put(FcConstant.ACCOUNT_ID_CONSTANT, "xxxx");
        keyValue.put(FcConstant.SERVICE_NAME_CONSTANT, "xxxx");
        keyValue.put(FcConstant.FUNCTION_NAME_CONSTANT, "xxxx");
        keyValue.put(FcConstant.INVOCATION_TYPE_CONSTANT, null);
        keyValue.put(FcConstant.QUALIFIER_CONSTANT, "LATEST");
        fcSinkTask.init(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("test fc");
        connectRecordList.add(connectRecord);
        fcSinkTask.start(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override
            public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {

            }

            @Override
            public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {

            }

            @Override
            public void pause(List<RecordPartition> partitions) {

            }

            @Override
            public void resume(List<RecordPartition> partitions) {

            }

            @Override
            public Set<RecordPartition> assignment() {
                return null;
            }
        });
        fcSinkTask.put(connectRecordList);
    }

}
