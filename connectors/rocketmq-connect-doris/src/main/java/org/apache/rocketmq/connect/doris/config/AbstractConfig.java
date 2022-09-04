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
package org.apache.rocketmq.connect.doris.config;
import io.openmessaging.KeyValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * abstract config
 */
public abstract class AbstractConfig {

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");

    // connection url
    public static final String CONNECTION_PREFIX = "connection.";
    public static final String CONNECTION_URL_CONFIG = CONNECTION_PREFIX + "url";
    // connection user
    public static final String CONNECTION_USER_CONFIG = CONNECTION_PREFIX + "user";
    private static final String CONNECTION_USER_DOC = "JDBC connection user.";
    // connection password
    public static final String CONNECTION_PASSWORD_CONFIG = CONNECTION_PREFIX + "password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
    // connection attempts
    public static final String CONNECTION_ATTEMPTS_CONFIG = CONNECTION_PREFIX + "attempts";
    public static final String CONNECTION_ATTEMPTS_DOC = "Maximum number of attempts to retrieve a valid JDBC connection.Must be a positive integer.";
    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;
    // backoff ms
    public static final String CONNECTION_BACKOFF_CONFIG = CONNECTION_PREFIX + "backoff.ms";
    public static final String CONNECTION_BACKOFF_DOC = "Backoff time in milliseconds between connection attempts.";
    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;
    /**
     * quote.sql.identifiers
     */
    public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
    public static final String QUOTE_SQL_IDENTIFIERS_DOC =
            "When to quote table names, column names, and other identifiers in SQL statements. "
                    + "For backward compatibility, the default is ``always``.";


    private String connectionDbUrl;
    private String connectionDbUser;
    private String connectionDbPassword;
    private Integer attempts;
    private Long backoffMs;
    private String quoteSqlIdentifiers;

    public AbstractConfig(KeyValue config) {
        connectionDbUrl = config.getString(CONNECTION_URL_CONFIG);
        connectionDbUser = config.getString(CONNECTION_USER_CONFIG);
        connectionDbPassword = config.getString(CONNECTION_PASSWORD_CONFIG);
        attempts = config.getInt(CONNECTION_ATTEMPTS_CONFIG, CONNECTION_ATTEMPTS_DEFAULT);
        backoffMs = config.getLong(CONNECTION_BACKOFF_CONFIG, CONNECTION_BACKOFF_DEFAULT);
    }


    public String getConnectionDbUrl() {
        return connectionDbUrl;
    }

    public String getConnectionDbUser() {
        return connectionDbUser;
    }

    public String getConnectionDbPassword() {
        return connectionDbPassword;
    }

    public Integer getAttempts() {
        return attempts;
    }

    public Long getBackoffMs() {
        return backoffMs;
    }

    public String getQuoteSqlIdentifiers() {
        return quoteSqlIdentifiers;
    }

    /**
     * get list
     *
     * @param config
     * @param key
     * @return
     */
    protected List<String> getList(KeyValue config, String key) {
        if (!config.containsKey(key) || Objects.isNull(config.getString(key))) {
            return new ArrayList<>();
        }
        return Arrays.asList(COMMA_WITH_WHITESPACE.split(config.getString(key), -1));
    }

    /**
     * get list
     *
     * @param config
     * @param key
     * @return
     */
    protected List<String> getList(KeyValue config, String key, String defaultValue) {
        if (config.containsKey(key) || Objects.isNull(config.getString(key))) {
            return Collections.singletonList(defaultValue);
        }
        return Arrays.asList(COMMA_WITH_WHITESPACE.split(config.getString(key), -1));
    }

    protected Boolean getBoolean(KeyValue config, String key, Boolean defaultValue) {
        return config.containsKey(key) ? Boolean.parseBoolean(config.getString(key)) : defaultValue;
    }

}

