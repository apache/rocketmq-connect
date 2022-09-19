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

import com.google.common.base.CaseFormat;
import io.openmessaging.KeyValue;

public class ChangeCaseConfig {
    public final CaseFormat from;
    public final CaseFormat to;

    public static final String FROM_CONFIG = "from";
    static final String FROM_DOC = "The format to move from ";
    public static final String TO_CONFIG = "to";
    static final String TO_DOC = "";

    public ChangeCaseConfig(KeyValue config) {
        String fromConfig = config.getString(FROM_CONFIG);
        this.from = CaseFormat.valueOf(fromConfig);
        String toConfig = config.getString(TO_CONFIG);
        this.to = CaseFormat.valueOf(toConfig);
    }

    /**
     * from
     *
     * @return
     */
    public CaseFormat from() {
        return this.from;
    }

    /**
     * to
     *
     * @return
     */
    public CaseFormat to() {
        return this.to;
    }
}
