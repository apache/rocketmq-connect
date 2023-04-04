package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LongConverterTest extends AbstractRecordConverterTest {


    @Before
    public void before() {
        this.recordConverter = new LongConverter();
    }


    @Test
    public void testIntegerConverter() {
        Long int64 = 64L;
        byte[] bytes = recordConverter.fromConnectData(TEST_TOPIC, SchemaBuilder.int64().build(), int64);
        SchemaAndValue schemaAndValue = recordConverter.toConnectData(TEST_TOPIC, bytes);
        Assert.assertEquals(schemaAndValue.value(), int64);
    }
}
