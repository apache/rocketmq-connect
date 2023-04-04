package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IntegerConverterTest extends AbstractRecordConverterTest {


    @Before
    public void before() {
        this.recordConverter = new IntegerConverter();
    }


    @Test
    public void testIntegerConverter() {
        Integer int32 = 32;
        byte[] bytes = recordConverter.fromConnectData(TEST_TOPIC, SchemaBuilder.int32().build(), int32);
        SchemaAndValue schemaAndValue = recordConverter.toConnectData(TEST_TOPIC, bytes);
        Assert.assertEquals(schemaAndValue.value(), int32);
    }
}
