package org.apache.rocketmq.connect.jdbc.connector.dialect;

import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectFactory;
import org.junit.Test;

public class DatabaseDialectFactoryTest {
    @Test
    public void findDialectFor() throws InterruptedException {
        DatabaseDialectFactory.findDialectFor("jdbc:mysql://*****:3306", null);
    }

    @Test
    public void findOpenMLDBDialectFor() throws InterruptedException {
        DatabaseDialectFactory.findDialectFor("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb", null);
    }
}
