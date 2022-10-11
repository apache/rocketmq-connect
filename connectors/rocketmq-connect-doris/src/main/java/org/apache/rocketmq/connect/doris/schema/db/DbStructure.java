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
package org.apache.rocketmq.connect.doris.schema.db;

import org.apache.rocketmq.connect.doris.connector.DorisSinkConfig;
import org.apache.rocketmq.connect.doris.schema.table.TableId;
import org.apache.rocketmq.connect.doris.sink.metadata.FieldsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;

/**
 *
 */
public class DbStructure {
    private static final Logger log = LoggerFactory.getLogger(DbStructure.class);

    /**
     * Create or amend table.
     *
     * @param config         the connector configuration
//     * @param connection     the database connection handle
     * @param tableId        the table ID
     * @param fieldsMetadata the fields metadata
     * @return whether a DDL operation was performed
     * @throws SQLException if a DDL operation was deemed necessary but failed
     */
    public boolean createOrAmendIfNecessary(
            final DorisSinkConfig config,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        // It seems that doris don't support create or amend table via stream load, so do nothing
        return false;
    }
}
