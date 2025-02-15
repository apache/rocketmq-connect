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

package org.apache.rocketmq.connect.neo4j.helper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.rocketmq.connect.neo4j.config.Neo4jBaseConfig;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jClient.class);

    private Neo4jBaseConfig neo4jBaseConfig;
    private Driver driver;

    public Neo4jClient(Neo4jBaseConfig config) {
        neo4jBaseConfig = config;
        createDriver(config);
    }

    private Driver createDriver(Neo4jBaseConfig config) {
        this.driver = GraphDatabase
            .driver("bolt://" + config.getNeo4jHost() + ":" + config.getNeo4jPort() + "/" + config.getNeo4jDataBase(),
                AuthTokens.basic(config.getNeo4jUser(), config.getNeo4jPassword()));
        return this.driver;
    }

    public void insert(String cql) {
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                final Result result = tx.run(cql);
                return null;
            });
        } catch (Exception e) {
            LOGGER.error(cql + " has error", e);
        }
    }

    public List<Record> query(String query) {
        try (Session session = driver.session()) {
            TransactionConfig txConfig = TransactionConfig.builder().withTimeout(Duration.ofSeconds(5)).build();
            final List<Record> recordList = session.readTransaction(tx -> {
                List<Record> list = new ArrayList<>();
                final Result result = tx.run(query);
                while (result.hasNext()) {
                    final Record record = result.next();
                    list.add(record);
                }
                return list;
            }, txConfig);
            return recordList;
        } catch (Exception e) {
            LOGGER.error(query + " has error", e);
        }
        return Collections.emptyList();
    }

    public boolean ping() {
        try (Session session = driver.session()) {
            session.readTransaction(tx -> {
                final Result result =
                    tx.run("use " + neo4jBaseConfig.getNeo4jDataBase() + " MATCH (a) RETURN a LIMIT 1");
                final List<Record> list = result.list();
                return "ok";
            });
        } catch (Exception e) {
            LOGGER.error("unable to ping to neo4j server.", e);
            return false;
        } catch (Throwable throwable) {
            LOGGER.error("unable to ping to neo4j server.", throwable);
            return false;
        }
        return true;
    }

    public List<String> getAllLabels() {
        try (Session session = driver.session()) {
            final List<String> labelList = session.readTransaction(tx -> {
                final Result result = tx.run("use " + neo4jBaseConfig.getNeo4jDataBase() + " CALL db.labels()");
                return result.list().stream().map(record -> record.get("label").asString())
                    .collect(Collectors.toList());
            });
            return labelList;
        } catch (Exception e) {
            LOGGER.error("CALL db.labels() has error", e);
        }
        return Collections.emptyList();
    }

    public List<String> getAllType() {
        try (Session session = driver.session()) {
            final List<String> labelList = session.readTransaction(tx -> {
                final Result result =
                    tx.run("use " + neo4jBaseConfig.getNeo4jDataBase() + " CALL db.relationshipTypes()");
                return result.list().stream().map(record -> record.get("relationshipType").asString())
                    .collect(Collectors.toList());
            });
            return labelList;
        } catch (Exception e) {
            LOGGER.error("CALL db.relationshipTypes() has error", e);
        }
        return Collections.emptyList();
    }
}