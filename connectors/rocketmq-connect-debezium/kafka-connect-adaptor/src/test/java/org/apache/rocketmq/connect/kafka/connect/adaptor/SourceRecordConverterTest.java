package org.apache.rocketmq.connect.kafka.connect.adaptor;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.RocketMQSourceSchemaConverter;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.RocketMQSourceValueConverter;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * source record converter test
 */
public class SourceRecordConverterTest {
    private SourceRecord originalRecord;

    @Before
    public void before() {
        // init record
        Schema schema = SchemaBuilder.struct()
                .field("field-string-01", Schema.INT64_SCHEMA)
                .field("field-boolean-02", Schema.BOOLEAN_SCHEMA)
                .field(
                        "field-struct-03",
                        SchemaBuilder.struct()
                                .field("field-struct-int64-01", Schema.INT64_SCHEMA)
                                .field("field-struct-boolean-02", Schema.BOOLEAN_SCHEMA)
                )
                .field("field-array-04-01", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("field-array-04-02",
                        SchemaBuilder.array(
                                SchemaBuilder.struct()
                                        .field("field-struct-string-01", Schema.STRING_SCHEMA)
                                        .field("field-struct-boolean-02", Schema.BOOLEAN_SCHEMA)
                        )
                )
                .field("field-map-05",
                        SchemaBuilder.map(
                                Schema.STRING_SCHEMA,
                                SchemaBuilder.struct()
                                        .field("field-struct-string-01", Schema.STRING_SCHEMA)
                                        .field("field-struct-boolean-02", Schema.BOOLEAN_SCHEMA)
                        )
                ).build();

        Struct struct = new Struct(schema);
        struct.put("field-string-01", 111111L);
        struct.put("field-boolean-02", true);

        Struct struct_01 = new Struct(schema.field("field-struct-03").schema());
        struct_01.put("field-struct-int64-01", 64l);
        struct_01.put("field-struct-boolean-02", true);
        // add struct
        struct.put("field-struct-03", struct_01);

        // add primate array
        List<String> strings = new ArrayList<>();
        strings.add("xxxx");
        struct.put("field-array-04-01", strings);

        // add struct array
        List<Struct> structs = new ArrayList<>();
        structs.add(new Struct(schema.field("field-array-04-02").schema().valueSchema())
                .put("field-struct-string-01", "scsdcsd")
                .put("field-struct-boolean-02", true));

        struct.put("field-array-04-02", structs);

        // add map

        Map<String, Struct> maps = new HashMap<>();
        Struct mapStruct = new Struct(schema.field("field-map-05").schema().valueSchema());
        mapStruct.put("field-struct-string-01", "scdsc");
        mapStruct.put("field-struct-boolean-02", true);
        maps.put("XXXX-test", mapStruct);
        struct.put("field-map-05", maps);

        originalRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "test-Topic", 1, schema, struct);
    }

    public Struct initStruct(Schema schema) {
        Struct struct = new Struct(schema);
        schema.fields().forEach(field -> {
            Schema.Type type = field.schema().type();
            switch (type) {
                case STRUCT:
                    struct.put(field.name(), initStruct(field.schema()));
                    break;
                case INT8:
                case INT16:
                case INT32:
                    struct.put(field.name(), 100);
                    break;
                case INT64:
                    struct.put(field.name(), 100000L);
                    break;
                case FLOAT32:
                    struct.put(field.name(), 100.00);
                    break;
                case FLOAT64:
                    struct.put(field.name(), 100000.00);
                    break;
                case BOOLEAN:
                    struct.put(field.name(), true);
                    break;
                case STRING:
                    struct.put(field.name(), "test");
                    break;
                case ARRAY:
                    struct.put(field.name(), initArray(schema.valueSchema()));
                    break;
                case MAP:
                    struct.put(field.name(), initMap(schema.keySchema(), schema.valueSchema()));
            }
        });
        return struct;
    }

    public Object initArray(Schema schema) {
        Schema.Type type = schema.type();
        switch (type) {
            case STRUCT:
                Struct[] structs = new Struct[1];
                structs[0] = initStruct(schema);
                return structs;
            case INT8:
            case INT16:
            case INT32:
                Integer[] integers = new Integer[1];
                integers[0] = 100;
                return integers;
            case INT64:
                Long[] longs = new Long[1];
                longs[0] = 1000000L;
                return longs;
            case FLOAT32:
                Float[] floats = new Float[1];
                floats[0] = 1000.00f;
                return floats;
            case FLOAT64:
                Double[] doubles = new Double[1];
                doubles[0] = 1000.00;
                return doubles;
            case BOOLEAN:
                Boolean[] booleans = new Boolean[1];
                booleans[0] = true;
                return booleans;
            case STRING:
                String[] strings = new String[1];
                strings[0] = "********XXXX";
                return strings;
            case ARRAY:
                Object[] obj = new Object[1];
                obj[0] = initArray(schema.valueSchema());
                return obj;
            case MAP:
                break;
        }
        return null;
    }

    private Object initMap(Schema keySchema, Schema valueSchema) {
        return null;
    }

    @Test
    public void testConverter() {
        // sourceRecord convert connect Record
        RocketMQSourceSchemaConverter rocketMQSourceSchemaConverter = new RocketMQSourceSchemaConverter(originalRecord.valueSchema());

        io.openmessaging.connector.api.data.Schema schema = rocketMQSourceSchemaConverter.schema();
        RocketMQSourceValueConverter rocketMQSourceValueConverter = new RocketMQSourceValueConverter();
        ConnectRecord connectRecord = new ConnectRecord(
                new RecordPartition(originalRecord.sourcePartition()),
                new RecordOffset(originalRecord.sourceOffset()),
                originalRecord.timestamp(),
                rocketMQSourceSchemaConverter.schema(),
                rocketMQSourceValueConverter.value(schema, originalRecord.value()));
        Iterator<Header> headers = originalRecord.headers().iterator();
        while (headers.hasNext()) {
            Header header = headers.next();
            if (header.schema().type().isPrimitive()) {
                connectRecord.addExtension(header.key(), (String) header.value());
            }
        }
        final byte[] messageBody = JSON.toJSONString(connectRecord).getBytes();
        String bodyStr = new String(messageBody, StandardCharsets.UTF_8);
        ConnectRecord newConnectRecord = JSON.parseObject(bodyStr, ConnectRecord.class);
    }
}
