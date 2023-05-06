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

package org.apache.rocketmq.connect.clickhouse.config;

public class ClickHouseConstants {
    public static final String CLICKHOUSE_HOST = "clickhousehost";

    public static final String CLICKHOUSE_PORT = "clickhouseport";

    public static final String CLICKHOUSE_DATABASE = "database";

    public static final String CLICKHOUSE_USERNAME = "username";

    public static final String CLICKHOUSE_PASSWORD = "password";

    public static final String CLICKHOUSE_ACCESSTOKEN = "accesstoken";

    public static final String CLICKHOUSE_TABLE = "table";

    public static final String TOPIC = "topic";

    public static final String CLICKHOUSE_OFFSET = "OFFSET";

    public static final String CLICKHOUSE_PARTITION = "CLICKHOUSE_PARTITION";

    public static final Integer timeoutSecondsDefault = 30;

    public static final int MILLI_IN_A_SEC = 1000;

    public static final Integer retryCountDefault = 3;

    public static final int MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME = 2000;

}
