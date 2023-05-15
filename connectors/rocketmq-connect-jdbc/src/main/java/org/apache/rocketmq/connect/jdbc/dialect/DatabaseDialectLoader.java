/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.jdbc.dialect;

import io.openmessaging.connector.api.errors.ConnectException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.util.JdbcUrlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatabaseDialectLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseDialectLoader.class);
    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");
    private static final Set<DatabaseDialectFactory> DATABASE_DIALECT_FACTORY = new HashSet<>();

    static {
        DATABASE_DIALECT_FACTORY.addAll(loadDatabaseDialectFactories());
    }

    /**
     * Get database dialect factory
     *
     * @param config
     * @return
     */
    public static DatabaseDialect getDatabaseDialect(AbstractConfig config) {
        String url = config.getConnectionDbUrl();
        assert url != null;
        JdbcUrlInfo jdbcUrlInfo = extractJdbcUrlInfo(url);
        final List<DatabaseDialectFactory> matchingFactories =
            DATABASE_DIALECT_FACTORY.stream().filter(f -> f.subProtocols().contains(jdbcUrlInfo.subprotocol())).collect(Collectors.toList());
        if (matchingFactories.isEmpty()) {
            throw new ConnectException(String.format("Cannot get database dialect by url [%s]", url));
        }
        return matchingFactories.get(0).create(config);
    }

    private static Set<DatabaseDialectFactory> loadDatabaseDialectFactories() {

        try {
            ClassLoader cl = DatabaseDialectFactory.class.getClassLoader();
            return AccessController.doPrivileged(new PrivilegedAction<Set<DatabaseDialectFactory>>() {
                public Set<DatabaseDialectFactory> run() {
                    final Set<DatabaseDialectFactory> result = new HashSet<>();
                    ServiceLoader<DatabaseDialectFactory> databaseDialectFactories = ServiceLoader.load(DatabaseDialectFactory.class, cl);
                    databaseDialectFactories.iterator().forEachRemaining(result::add);
                    return result;
                }
            });
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for jdbc dialects factory.", e);
            throw new ConnectException("Could not load service provider for jdbc dialects factory.", e);
        }
    }

    static JdbcUrlInfo extractJdbcUrlInfo(final String url) {
        LOG.info("Validating JDBC URL.");
        Matcher matcher = PROTOCOL_PATTERN.matcher(url);
        if (matcher.matches()) {
            LOG.info("Validated JDBC URL.");
            return new JdbcUrlDetails(matcher.group(1), matcher.group(2), url);
        }
        LOG.error("Not a valid JDBC URL: " + url);
        throw new ConnectException("Not a valid JDBC URL: " + url);
    }

    /**
     * Jdbc url details
     */
    static class JdbcUrlDetails implements JdbcUrlInfo {
        final String subprotocol;
        final String subname;
        final String url;

        public JdbcUrlDetails(String subprotocol, String subname, String url) {
            this.subprotocol = subprotocol;
            this.subname = subname;
            this.url = url;
        }

        @Override
        public String subprotocol() {
            return subprotocol;
        }

        @Override
        public String subname() {
            return subname;
        }

        @Override
        public String url() {
            return url;
        }

        @Override
        public String toString() {
            return "JDBC subprotocol '" + subprotocol + "' and source '" + url + "'";
        }
    }

}
