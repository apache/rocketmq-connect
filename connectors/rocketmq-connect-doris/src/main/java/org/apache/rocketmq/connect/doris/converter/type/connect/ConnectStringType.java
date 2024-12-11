/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.converter.type.connect;

import io.openmessaging.connector.api.data.Schema;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisType;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisTypeProperties;

public class ConnectStringType extends AbstractConnectSchemaType {

    public static final ConnectStringType INSTANCE = new ConnectStringType();

    @Override
    public String getTypeName(Schema schema) {
        int columnLength = getColumnLength(schema);
        if (columnLength > 0) {
            return columnLength * 3 > DorisTypeProperties.MAX_VARCHAR_SIZE
                ? DorisType.STRING
                : String.format("%s(%s)", DorisType.VARCHAR, columnLength * 3);
        }
        return DorisType.STRING;
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {"STRING"};
    }

    private int getColumnLength(Schema schema) {
        return Integer.parseInt(getSourceColumnLength(schema).orElse("0"));
    }
}
