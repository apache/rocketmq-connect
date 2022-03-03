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


package org.apache.rocketmq.connect.cassandra.schema.column;

import io.openmessaging.connector.api.data.FieldType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ColumnParser {


    /**
     * Currently this class implementation is not complete yet.
     * @param dataType
     * @param colType
     * @param charset
     * @return
     */
    public static ColumnParser getColumnParser(String dataType, String colType, String charset) {

        switch (dataType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "varint":
            case "int":
                return new IntColumnParser(dataType, colType);
            case "bigint":
                return new BigIntColumnParser(colType);
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "varchar":
            case "ascii":
            case "char":
                return new StringColumnParser(charset);
            case "date":
            case "datetime":
            case "timestamp":
                return new DateTimeColumnParser();
            case "time":
                return new TimeColumnParser();
            case "year":
                return new YearColumnParser();
            case "enum":
                return new EnumColumnParser(colType);
            case "set":
                return new SetColumnParser(colType);
            case "boolean":
                return new BooleanColumnParser();
            default:
                return new DefaultColumnParser();
        }
    }

    public static FieldType mapConnectorFieldType(String dataType) {

        switch (dataType) {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
                return FieldType.INT32;
            case "bigint":
                return FieldType.BIG_INTEGER;
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "varchar":
            case "char":
                return FieldType.STRING;
            case "date":
            case "datetime":
            case "timestamp":
            case "time":
            case "year":
                return FieldType.DATETIME;
            case "enum":
                return null;
            case "set":
                return null;
            default:
                return FieldType.BYTES;
        }
    }

    public static String[] extractEnumValues(String colType) {
        String[] enumValues = {};
        Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(colType);
        if (matcher.matches()) {
            enumValues = matcher.group(2).replace("'", "").split(",");
        }

        return enumValues;
    }

    public abstract Object getValue(Object value);

}
