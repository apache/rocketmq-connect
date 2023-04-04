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
package org.apache.rocketmq.connect.doris.util;

//import org.apache.rocketmq.connect.doris.connector.DorisSourceConfig;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum NumericMapping {
    NONE,
    PRECISION_ONLY,
    BEST_FIT,
    BEST_FIT_EAGER_DOUBLE;

    private static final Map<String, NumericMapping> REVERSE = new HashMap<>(values().length);

    static {
        for (NumericMapping val : values()) {
            REVERSE.put(val.name().toLowerCase(Locale.ROOT), val);
        }
    }

    public static NumericMapping get(String prop) {
        // not adding a check for null value because the recommender/validator should catch those.
        return REVERSE.get(prop.toLowerCase(Locale.ROOT));
    }

//    public static NumericMapping get(JdbcSourceConfig config) {
//        // We use 'null' as default to be able to check the old config if the new one is unset.
//        if (config.getNumericMapping() != null) {
//            return NumericMapping.valueOf(config.getNumericMapping());
//        }
//        if (config.getNumericPrecisionMapping()) {
//            return NumericMapping.PRECISION_ONLY;
//        }
//        return NumericMapping.NONE;
//    }
}