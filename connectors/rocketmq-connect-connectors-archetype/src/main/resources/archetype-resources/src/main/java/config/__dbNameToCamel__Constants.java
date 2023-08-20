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

package ${package}.config;

public class ${dbNameToCamel}Constants {
    public static final String ${dbNameToUpperCase}_HOST = "${dbNameToLowerCase}host";

    public static final String ${dbNameToUpperCase}_PORT = "${dbNameToLowerCase}port";

    public static final String ${dbNameToUpperCase}_DATABASE = "database";

    public static final String ${dbNameToUpperCase}_USERNAME = "username";

    public static final String ${dbNameToUpperCase}_PASSWORD = "password";

    public static final String ${dbNameToUpperCase}_TABLE = "table";

    public static final String TOPIC = "topic";

    public static final String ${dbNameToUpperCase}_OFFSET = "OFFSET";

    public static final String ${dbNameToUpperCase}_PARTITION = "${dbNameToUpperCase}_PARTITION";

    public static final Integer defaultTimeoutSeconds = 30;

    public static final int MILLI_IN_A_SEC = 1000;

    public static final Integer retryCountDefault = 3;

    public static final int BATCH_SIZE = 2000;

}
