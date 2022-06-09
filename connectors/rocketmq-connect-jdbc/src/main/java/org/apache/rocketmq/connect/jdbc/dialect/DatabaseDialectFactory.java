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
package org.apache.rocketmq.connect.jdbc.dialect;

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.provider.DatabaseDialectProvider;
import org.apache.rocketmq.connect.jdbc.dialect.provider.JdbcUrlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * load and find database dialect
 */
public class DatabaseDialectFactory {

    /**
     * regex jdbc protocol
     */
    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseDialectFactory.class);
    private static final ConcurrentMap<String, DatabaseDialectProvider> REGISTRY = new ConcurrentSkipListMap<>();

    static {
        loadAllDialects();
    }

    /**
     * load all database dialect
     */
    private static void loadAllDialects() {
        LOG.debug("Searching for and loading all JDBC source dialects on the classpath");
        final AtomicInteger count = new AtomicInteger();
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                ServiceLoader<DatabaseDialectProvider> loadedDialects =
                        ServiceLoader.load(
                                DatabaseDialectProvider.class,
                                this.getClass().getClassLoader()
                        );
                Iterator<DatabaseDialectProvider> dialectIterator = loadedDialects.iterator();
                try {
                    while (dialectIterator.hasNext()) {
                        try {
                            DatabaseDialectProvider provider = dialectIterator.next();
                            REGISTRY.put(provider.getClass().getName(), provider);
                            count.incrementAndGet();
                            LOG.debug("Found '{}' provider {}", provider, provider.getClass());
                        } catch (Throwable t) {
                            LOG.debug("Skipping dialect provider after error while loading", t);
                        }
                    }
                } catch (Throwable t) {
                    LOG.debug("Error loading dialect providers", t);
                }
                return null;
            }
        });
        LOG.debug("Registered {} source dialects", count.get());
    }


    /**
     * find dialect for
     *
     * @param jdbcUrl
     * @param config
     * @return
     * @throws ConnectException
     */
    public static DatabaseDialect findDialectFor(
            String jdbcUrl,
            AbstractConfig config
    ) throws ConnectException {
        final JdbcUrlInfo info = extractJdbcUrlInfo(jdbcUrl);
        LOG.debug("Finding best dialect for {}", info);
        DatabaseDialectProvider bestMatch = null;
        for (DatabaseDialectProvider provider : REGISTRY.values()) {
            if (provider.protocolName().equals(info.subProtocol())) {
                bestMatch = provider;
                break;
            }
        }
        LOG.debug("Using dialect {}  against {}", bestMatch, info);
        return bestMatch.create(config);
    }

    /**
     * create database dialect
     *
     * @param dialectName
     * @param config
     * @return
     * @throws ConnectException
     */
    public static DatabaseDialect create(
            String dialectName,
            AbstractConfig config
    ) throws ConnectException {
        LOG.debug("Looking for named dialect '{}'", dialectName);
        Set<String> dialectNames = new HashSet<>();
        for (DatabaseDialectProvider provider : REGISTRY.values()) {
            dialectNames.add(provider.dialectName());
            if (provider.dialectName().equals(dialectName)) {
                return provider.create(config);
            }
        }
        for (DatabaseDialectProvider provider : REGISTRY.values()) {
            if (provider.dialectName().equalsIgnoreCase(dialectName)) {
                return provider.create(config);
            }
        }

        throw new ConnectException(
                "Unable to find dialect with name '" + dialectName + "' in the available dialects: "
                        + dialectNames
        );
    }

    public static JdbcUrlInfo extractJdbcUrlInfo(final String url) {
        Matcher matcher = PROTOCOL_PATTERN.matcher(url);
        if (matcher.matches()) {
            return new JdbcUrlDetails(matcher.group(1), matcher.group(2), url);
        }
        throw new ConnectException("Not a valid JDBC URL: " + url);
    }


    static class JdbcUrlDetails implements JdbcUrlInfo {
        final String subprotocol;
        final String subname;
        final String url;

        public JdbcUrlDetails(
                String subprotocol,
                String subname,
                String url
        ) {
            this.subprotocol = subprotocol;
            this.subname = subname;
            this.url = url;
        }

        @Override
        public String subProtocol() {
            return subprotocol;
        }

        @Override
        public String subName() {
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

    private DatabaseDialectFactory() {
    }
}
