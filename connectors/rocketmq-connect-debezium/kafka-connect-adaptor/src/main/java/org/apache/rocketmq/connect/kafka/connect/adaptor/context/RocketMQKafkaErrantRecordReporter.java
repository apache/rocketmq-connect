package org.apache.rocketmq.connect.kafka.connect.adaptor.context;

import io.openmessaging.connector.api.component.task.sink.ErrorRecordReporter;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.Converters;

import java.util.concurrent.Future;

/**
 * rocketmq kafka error record reporter
 */
public class RocketMQKafkaErrantRecordReporter implements ErrantRecordReporter {
    final ErrorRecordReporter errorRecordReporter;

    RocketMQKafkaErrantRecordReporter(SinkTaskContext context) {
        errorRecordReporter = context.errorRecordReporter();
    }

    @Override
    public Future<Void> report(SinkRecord sinkRecord, Throwable throwable) {
        // 数据转换
        ConnectRecord record = Converters.fromSinkRecord(sinkRecord);
        errorRecordReporter.report(record, throwable);
        return null;
    }
}
