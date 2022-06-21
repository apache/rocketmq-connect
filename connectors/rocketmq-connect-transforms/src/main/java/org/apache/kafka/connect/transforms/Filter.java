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

package org.apache.kafka.connect.transforms;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.ComponentContext;
import io.openmessaging.connector.api.data.ConnectRecord;

/**
 * Drops all records, filtering them from subsequent transformations in the chain.
 * This is intended to be used conditionally to filter out records matching (or not matching)
 */
public abstract class  Filter<R extends ConnectRecord> extends BaseTransformation<R> {


    @Override
    public void stop() {

    }

    @Override
    public R doTransform(R r) {
        return null;
    }

}
