package org.apache.rocketmq.connect.jdbc.connector.dialect;

import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectFactory;
import org.junit.Test;

/**
 * database dialect factory test
 */
public class DatabaseDialectFactoryTest {
    @Test
    public void findDialectFor() throws InterruptedException {
        DatabaseDialectFactory.findDialectFor("jdbc:mysql://*****:3306", null);
    }
}
