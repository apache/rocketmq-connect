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
package org.apache.rocketmq.connect.jdbc.config;

import com.google.common.collect.Lists;
import io.openmessaging.KeyValue;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.rocketmq.connect.jdbc.util.QuoteMethod;

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
    // connection password
    public static final String CONNECTION_PASSWORD_CONFIG = CONNECTION_PREFIX + "password";
    // connection attempts
    public static final String CONNECTION_ATTEMPTS_CONFIG = CONNECTION_PREFIX + "attempts";

    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;
    // backoff ms
    public static final String CONNECTION_BACKOFF_CONFIG = CONNECTION_PREFIX + "backoff.ms";

    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;
    /**
     * quote.sql.identifiers
     */
    public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
    public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = QuoteMethod.ALWAYS.name().toString();


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
        quoteSqlIdentifiers = config.getString(QUOTE_SQL_IDENTIFIERS_CONFIG, QUOTE_SQL_IDENTIFIERS_DEFAULT);
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
            return Lists.newArrayList();
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
            return Lists.newArrayList(defaultValue);
        }
        return Arrays.asList(COMMA_WITH_WHITESPACE.split(config.getString(key), -1));
    }

    protected Boolean getBoolean(KeyValue config, String key, Boolean defaultValue) {
        return config.containsKey(key) ? Boolean.parseBoolean(config.getString(key)) : defaultValue;
    }

}
