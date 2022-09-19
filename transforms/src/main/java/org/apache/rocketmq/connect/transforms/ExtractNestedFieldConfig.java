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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.connect.transforms;

import io.openmessaging.KeyValue;

/**
 * extract nested field config
 */
public class ExtractNestedFieldConfig {
    public final String outerFieldName;
    public final String innerFieldName;
    public final String outputFieldName;

    public ExtractNestedFieldConfig(KeyValue config) {
        this.outerFieldName = config.getString(OUTER_FIELD_NAME_CONF);
        this.innerFieldName = config.getString(INNER_FIELD_NAME_CONF);
        this.outputFieldName = config.getString(OUTPUT_FIELD_NAME_CONF);
    }

    public static final String OUTER_FIELD_NAME_CONF = "input.outer.field.name";
    static final String OUTER_FIELD_NAME_DOC = "The field on the parent struct containing the child struct. " +
        "For example if you wanted the extract `address.state` you would use `address`.";
    public static final String INNER_FIELD_NAME_CONF = "input.inner.field.name";
    static final String INNER_FIELD_NAME_DOC = "The field on the child struct containing the field to be extracted. " +
        "For example if you wanted the extract `address.state` you would use `state`.";
    public static final String OUTPUT_FIELD_NAME_CONF = "output.field.name";
    static final String OUTPUT_FIELD_NAME_DOC = "The field to place the extracted value into.";

    public String outerFieldName() {
        return this.outerFieldName;
    }

    public String innerFieldName() {
        return this.innerFieldName;
    }

    public String outputFieldName() {
        return this.outerFieldName;
    }

}
