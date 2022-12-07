package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StringConverterTest extends AbstractRecordConverterTest {


    @Before
    public void before() {
        this.recordConverter = new StringConverter();
    }


    @Test
    public void testStringConverter() {
        String str = "test_converter";
        byte[] bytes = recordConverter.fromConnectData(TEST_TOPIC, SchemaBuilder.string().build(), str);
        SchemaAndValue schemaAndValue = recordConverter.toConnectData(TEST_TOPIC, bytes);
        Assert.assertEquals(schemaAndValue.value(), str);
    }
}
