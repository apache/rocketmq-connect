package org.apache.rocketmq.connect.hologres.connector;

import com.alibaba.hologres.client.BinlogShardGroupReader;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Subscribe;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.binlog.BinlogHeartBeatRecord;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import com.alibaba.hologres.com.google.common.base.Strings;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.connect.hologres.config.HologresSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.rocketmq.connect.hologres.config.HologresConstant.PARTITION_INFO_KEY;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.PARTITION_INDEX_KEY;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.HOLOGRES_POSITION;


public class HologresSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(HologresSourceTask.class);

    private KeyValue keyValue;
    private HologresSourceConfig sourceConfig;
    private HoloConfig holoClientConfig;
    private HoloClient holoClient;
    private BinlogShardGroupReader reader;
    private long count = 0;

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        List<ConnectRecord> records = new ArrayList<>();
        try {
            BinlogRecord record = reader.getBinlogRecord();

            if (record instanceof BinlogHeartBeatRecord) {
                return null;
            }

            if (++count % 1000 == 0) {
                reader.commit(sourceConfig.getBinlogCommitTimeIntervalMs());
            }

            records.add(hologresRecord2ConnectRecord(record));
        } catch (Exception e) {
            log.error("Error while polling data from Hologres", e);
        }
        return records;
    }

    private ConnectRecord hologresRecord2ConnectRecord(BinlogRecord record) {
        List<Field> fields = buildFields(record);
        Schema schema = SchemaBuilder.struct()
                .name(record.getTableName().getTableName())
                .build();
        schema.setFields(fields);
        return new ConnectRecord(
                buildRecordPartition(record),
                buildRecordOffset(record),
                System.currentTimeMillis(),
                schema,
                buildPayLoad(fields, schema, record));
    }

    private RecordPartition buildRecordPartition(BinlogRecord record) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(PARTITION_INFO_KEY, record.getSchema().getPartitionInfo());
        partitionMap.put(PARTITION_INDEX_KEY, String.valueOf(record.getSchema().getPartitionIndex()));
        return new RecordPartition(partitionMap);
    }

    private RecordOffset buildRecordOffset(BinlogRecord record) {
        Map<String, Object> offsetMap = new HashMap<>();
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        offsetMap.put(record.getTableName() + ":" + HOLOGRES_POSITION, record.getBinlogLsn());
        return recordOffset;
    }

    private List<Field> buildFields(BinlogRecord record) {
        List<Field> fields = new ArrayList<>();
        final Column[] columns = record.getSchema().getColumnSchema();

        for (int i = 0; i < columns.length; ++i) {
            fields.add(new Field(i, columns[i].getName(), getSchema(record.getObject(i))));
        }

        return fields;
    }

    public Schema getSchema(Object obj) {
        if (obj instanceof Integer) {
            return SchemaBuilder.int32().build();
        } else if (obj instanceof Long) {
            return SchemaBuilder.int64().build();
        } else if (obj instanceof String) {
            return SchemaBuilder.string().build();
        } else if (obj instanceof Date) {
            return SchemaBuilder.time().build();
        } else if (obj instanceof Timestamp) {
            return SchemaBuilder.timestamp().build();
        } else if (obj instanceof Boolean) {
            return SchemaBuilder.bool().build();
        }
        return SchemaBuilder.string().build();
    }

    private Struct buildPayLoad(List<Field> fields, Schema schema, BinlogRecord record) {
        Struct payLoad = new Struct(schema);
        for (int i = 0; i < fields.size(); ++i) {
            payLoad.put(fields.get(i), record.getValues()[i]);
        }
        return payLoad;
    }

    @Override
    public void start(KeyValue keyValue) {
        this.keyValue = keyValue;
        this.sourceConfig = new HologresSourceConfig(keyValue);
        this.holoClientConfig = buildHoloConfig(sourceConfig);
        checkConfigValidate();

        log.info("Initializing hologres client and binlog subscribe");
        try {
            this.holoClient = new HoloClient(holoClientConfig);
            this.reader = holoClient.binlogSubscribe(Subscribe
                    .newStartTimeBuilder(sourceConfig.getTable(), sourceConfig.getSlotName())
                    .setBinlogReadStartTime(sourceConfig.getStartTime())
                    .build());
        } catch (HoloClientException e) {
            log.error("Init hologres client failed", e);
            throw new RuntimeException(e);
        }
    }

    private void checkConfigValidate() {
        if (Strings.isNullOrEmpty(sourceConfig.getSlotName())) {
            throw new RuntimeException("Hologres source connector must set slot name.");
        }

        if (Strings.isNullOrEmpty(sourceConfig.getStartTime())) {
            throw new RuntimeException("Hologres source connector must set start time.");
        }
    }

    private HoloConfig buildHoloConfig(HologresSourceConfig sourceConfig) {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(sourceConfig.getJdbcUrl());
        holoConfig.setUsername(sourceConfig.getUsername());
        holoConfig.setPassword(sourceConfig.getPassword());

        // binlog optional configuration
        holoConfig.setBinlogReadBatchSize(sourceConfig.getBinlogReadBatchSize());
        holoConfig.setBinlogIgnoreDelete(sourceConfig.isBinlogIgnoreDelete());
        holoConfig.setBinlogIgnoreBeforeUpdate(sourceConfig.isBinlogIgnoreBeforeUpdate());
        holoConfig.setBinlogHeartBeatIntervalMs(sourceConfig.getBinlogHeartBeatIntervalMs());
        holoConfig.setRetryCount(sourceConfig.getRetryCount());

        return holoConfig;
    }

    public void stop() {
        log.info("Stopping HologresSourceTask");
    }
}
