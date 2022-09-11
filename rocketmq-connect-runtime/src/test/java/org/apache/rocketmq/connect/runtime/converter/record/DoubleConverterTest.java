package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DoubleConverterTest extends AbstractRecordConverterTest {


    @Before
    public void before() {
        this.recordConverter = new DoubleConverter();
    }


    @Test
    public void testDoubleConverter() {
        Double float64 = 32.00;
        byte[] bytes = recordConverter.fromConnectData(TEST_TOPIC, SchemaBuilder.float64().build(), float64);
        SchemaAndValue schemaAndValue = recordConverter.toConnectData(TEST_TOPIC, bytes);
        Assert.assertEquals(schemaAndValue.value(), float64);
    }
}
