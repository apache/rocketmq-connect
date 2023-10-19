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

package org.apache.rocketmq.connect.hologres.config;

public class HologresConstant {

    public static String TASK_NUM = "tasks.num";
    public static final String DRIVER_NAME = "com.aliyun.hologres.jdbc.HoloDriver";
    public static final String URL_PREFIX = "jdbc:hologres://";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String TABLES = "tables";
    public static final String TABLE_NAME = "tableName";

    // source related
    public static final String SOURCE_BATCH_SIZE = "binlogReadBatchSize";
    public static final int SOURCE_BATCH_SIZE_DEFAULT = 1024;
    public static final String SOURCE_HEARTBEAT_INTERVAL = "binlogHeartBeatIntervalMs";
    public static final int SOURCE_HEARTBEAT_INTERVAL_DEFAULT = -1;
    public static final String SOURCE_IGNORE_DELETE = "binlogIgnoreDelete";
    public static final String SOURCE_IGNORE_DELETE_DEFAULT = "false";
    public static final String SOURCE_IGNORE_BEFORE_UPDATE = "binlogIgnoreBeforeUpdate";
    public static final String SOURCE_IGNORE_BEFORE_UPDATE_DEFAULT = "false";
    public static final String SOURCE_RETRY_COUNT = "retryCount";
    public static final int SOURCE_RETRY_COUNT_DEFAULT = 3;
    public static final String SOURCE_COMMIT_TIME_INTERVAL = "binlogCommitIntervalMs";
    public static final int SOURCE_COMMIT_TIME_INTERVAL_DEFAULT = 5000;
    public static final String SOURCE_SLOT_NAME = "slotName";
    public static final String PARTITION_INFO_KEY = "partitionInfo";
    public static final String PARTITION_INDEX_KEY = "partitionIndex";
    public static final String HOLOGRES_POSITION = "HOLOGRES_POSITION";

    // sink related
    public static final String DYNAMIC_PARTITION = "dynamicPartition";
    public static final String DYNAMIC_PARTITION_DEFAULT = "false";
    public static final String WRITE_MODE = "writeMode";
    public static final String WRITE_MODE_DEFAULT = "INSERT_OR_REPLACE";

    public static final String WRITE_BATCH = "writeBatch";
    public static final int WRITE_BATCH_DEFAULT = 512;

}
