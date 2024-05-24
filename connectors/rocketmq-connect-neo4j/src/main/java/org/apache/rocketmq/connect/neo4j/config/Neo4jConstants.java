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
package org.apache.rocketmq.connect.neo4j.config;

public class Neo4jConstants {
    public static final String NEO4J_HOST = "neo4jHost";

    public static final String NEO4J_PORT = "neo4jPort";

    public static final String NEO4J_USER = "neo4jUser";

    public static final String NEO4J_PASSWORD = "neo4jPassword";

    public static final String NEO4J_TOPIC = "topic";

    public static final String NEO4J_OFFSET = "OFFSET";

    public static final String NEO4J_PARTITION = "NEO4J_PARTITION";

    public static final String NEO4J_DB = "neo4jDataBase";

    public static final String LABEL_TYPE = "labelType";

    public static final String LABELS = "labels";

    public static final String LABEL = "label";

    public static final int timeout_Seconds_Default = 30;

    public static final int MILLI_IN_A_SEC = 1000;

    public static final int retry_Count_Default = 3;

    public static final int MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME = 2000;

    public static final String COLUMN = "column";
    public static final String COLUMN_NAME = "name";
    public static final String VALUE_TYPE = "type";
    public static final String COLUMN_TYPE = "columnType";
    public static final String VALUE_EXTRACT = "valueExtract";

    public enum ColumnType {
        /**
         * node or relationship id
         */
        primaryKey,

        /**
         * node label or relationship type
         */
        primaryLabel,

        /**
         * node property
         */
        nodeProperty,

        /**
         * collects all node property to Json list
         */
        nodeJsonProperty,

        /**
         * start node id of relationship
         */
        srcPrimaryKey,

        /**
         * start node label of relationship
         */
        srcPrimaryLabel,

        /**
         * end node id of relationship
         */
        dstPrimaryKey,

        /**
         * end node label of relationship
         */
        dstPrimaryLabel,

        /**
         * relationship property
         */
        relationshipProperty,

        /**
         * collects all relationship property to Json list
         */
        relationshipJsonProperty,
    }
}