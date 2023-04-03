/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.hive.replicator.source;

import java.sql.SQLException;
import java.sql.Statement;
import org.apache.rocketmq.connect.hive.config.HiveConfig;
import org.apache.rocketmq.connect.hive.config.HiveJdbcDriverManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HiveQueryTest {

    private Statement stmt;

    private HiveConfig config;

    @Before
    public void before() {
        config = new HiveConfig();
        config.setHost("localhost");
        config.setPort(10000);
        config.setDatabase("default");
        HiveJdbcDriverManager.init(config);
        stmt = HiveJdbcDriverManager.getStatement();
    }

    @Test
    public void countDataTest() throws SQLException {
        final int count = HiveQuery.countData(stmt, "invites", "foo", "1");
        Assert.assertNotNull(count);
    }

    @Test
    public void selectSpecifiedIndexDataTest() throws SQLException {
        final String value = HiveQuery.selectSpecifiedIndexData(stmt, "invites", "foo", "1", 0);
        Assert.assertNotNull(value);
    }

}
