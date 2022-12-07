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
package org.apache.rocketmq.schema.json;

import org.apache.rocketmq.schema.common.AbstractConverterConfig;

import java.util.Locale;
import java.util.Map;

public class JsonSchemaConverterConfig extends AbstractConverterConfig {
    public static final String USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG = "use.optional.for.nonrequired";
    public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
    /**
     * validate schema enabled
     */
    public static final String VALIDATE_ENABLED = "validate.enabled";
    private static final boolean USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT = false;
    private static final DecimalFormat DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64;
    private static final boolean VALIDATE_ENABLED_DEFAULT = true;

    private final Map<String, ?> props;

    public JsonSchemaConverterConfig(Map<String, ?> props) {
        super(props);
        this.props = props;
    }


    public boolean validate() {
        return props.containsKey(VALIDATE_ENABLED) ?
                Boolean.valueOf(props.get(VALIDATE_ENABLED).toString()) : VALIDATE_ENABLED_DEFAULT;
    }

    public boolean useOptionalForNonRequiredProperties() {
        return props.containsKey(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG) ?
                Boolean.valueOf(props.get(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG).toString()) : USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT;
    }

    /**
     * decimal format
     *
     * @return
     */
    public DecimalFormat decimalFormat() {
        return props.containsKey(DECIMAL_FORMAT_CONFIG) ?
                DecimalFormat.valueOf(props.get(DECIMAL_FORMAT_CONFIG).toString().toUpperCase(Locale.ROOT)) : DECIMAL_FORMAT_DEFAULT;

    }

}
