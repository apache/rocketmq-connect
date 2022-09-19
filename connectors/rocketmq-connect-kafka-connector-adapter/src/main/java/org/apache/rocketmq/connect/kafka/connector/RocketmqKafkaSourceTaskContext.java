package org.apache.rocketmq.connect.kafka.connector;


import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.rocketmq.connect.kafka.util.ConfigUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RocketmqKafkaSourceTaskContext implements org.apache.kafka.connect.source.SourceTaskContext {

    private SourceTaskContext sourceTaskContext;

    public RocketmqKafkaSourceTaskContext(SourceTaskContext sourceTaskContext) {
        this.sourceTaskContext = sourceTaskContext;
    }

    @Override
    public Map<String, String> configs() {
        return ConfigUtil.keyValueConfigToMap(sourceTaskContext.configs());
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return new OffsetStorageReader(){
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return offsets(Collections.singletonList(partition)).get(partition);
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {

                Collection<RecordPartition> rocketmqPartitions = partitions.stream()
                        .map(RecordPartition::new)
                        .collect(Collectors.toList());

                Map<Map<String, T>, Map<String, Object>> results = new HashMap<>(partitions.size());
                sourceTaskContext
                        .offsetStorageReader()
                        .readOffsets(rocketmqPartitions)
                        .forEach((p,o) -> {
                            results.put((Map<String, T>)p.getPartition(), mayConvertToLongOffset(o.getOffset()));
                        });
                return results;
            }

            // kafka的offset是long表示，被序列化再反序列化会是int
            private Map<String, Object> mayConvertToLongOffset(Map<String, ?> offset){
                Map<String, Object> result = new HashMap<>(offset.size());
                for(Map.Entry<String, ?> kv: offset.entrySet()){
                    Object v = kv.getValue();
                    result.put(kv.getKey(), v instanceof Integer ?  ((Integer) v).longValue():v);
                }
                return result;
            }
        };
    }
}
