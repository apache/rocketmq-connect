#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package}.helper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import ${package}.config.${dbNameToCamel}Constants;
import ${package}.config.${dbNameToCamel}BaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ${dbNameToCamel}HelperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(${dbNameToCamel}HelperClient.class);

    private ${dbNameToCamel}BaseConfig config;
    private int timeout = ${dbNameToCamel}Constants.defaultTimeoutSeconds * ${dbNameToCamel}Constants.MILLI_IN_A_SEC;
    private int retry = ${dbNameToCamel}Constants.retryCountDefault;

    public ${dbNameToCamel}HelperClient(${dbNameToCamel}BaseConfig config) {
        this.config = config;
        initConnection();
    }

    public void initConnection() {
        // FIXME: Write your code here
        throw new RuntimeException("Method not implemented");
    }

    public boolean ping() {
        // FIXME: Write your code here
        throw new RuntimeException("Method not implemented");
    }

    public List<?> query(long offset, int batchSize) {
        // FIXME: Write your code here
        throw new RuntimeException("Method not implemented");
    }

    public void batchInsert(List<?>) {
        // FIXME: Write your code here
        throw new RuntimeException("Method not implemented");
    }

    public boolean stop() {
        // FIXME: Write your code here
        throw new RuntimeException("Method not implemented");
    }
}
