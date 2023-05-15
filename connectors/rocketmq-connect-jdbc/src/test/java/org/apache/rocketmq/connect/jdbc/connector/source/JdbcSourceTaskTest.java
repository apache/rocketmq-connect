package org.apache.rocketmq.connect.jdbc.connector.source;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.jdbc.source.JdbcSourceTaskConfig;
import org.junit.Test;

/**
 * jdbc source task
 */
public class JdbcSourceTaskTest {
    // jdbc source task
    KeyValue conf = new DefaultKeyValue();

    public void init() {
        conf.put(JdbcSourceTaskConfig.CONNECTION_URL_CONFIG, "");
        conf.put(JdbcSourceTaskConfig.CONNECTION_USER_CONFIG, "");
        conf.put(JdbcSourceTaskConfig.CONNECTION_PASSWORD_CONFIG, "");
    }

    @Test
    public void pollTest() {

    }
}
