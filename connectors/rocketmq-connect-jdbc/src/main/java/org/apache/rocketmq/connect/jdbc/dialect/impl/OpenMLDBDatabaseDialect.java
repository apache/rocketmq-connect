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
package org.apache.rocketmq.connect.jdbc.dialect.impl;

import lombok.SneakyThrows;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.dialect.provider.DatabaseDialectProvider;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * openmldb database dialect
 */
public class OpenMLDBDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(OpenMLDBDatabaseDialect.class);

  /**
   * The provider for {@link OpenMLDBDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider {
    @SneakyThrows
    public Provider() {
      super(OpenMLDBDatabaseDialect.class.getSimpleName(), "openmldb");
      Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new OpenMLDBDatabaseDialect(config);
    }
  }

  /**
   * create openMLDB database dialect
   * @param config
   */
  public OpenMLDBDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "`", "`"));
  }


  @Override
  protected String sanitizedUrl(String url) {
    // MySQL can also have "username:password@" at the beginning of the host list and
    // in parenthetical properties
    return super.sanitizedUrl(url)
                .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
                .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
  }
}
