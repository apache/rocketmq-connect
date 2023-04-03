package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShortConverterTest extends AbstractRecordConverterTest {


    @Before
    public void before() {
        this.recordConverter = new ShortConverter();
    }


    @Test
    public void testShortConverter() {
        Short int16 = 16;
        byte[] bytes = recordConverter.fromConnectData(TEST_TOPIC, SchemaBuilder.int16().build(), int16);
        SchemaAndValue schemaAndValue = recordConverter.toConnectData(TEST_TOPIC, bytes);
        Assert.assertEquals(schemaAndValue.value(), int16);
    }
}
