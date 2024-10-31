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

package org.apache.rocketmq.connect.doris.writer;

import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * record buffer
 */
public class RecordBuffer extends PartitionBuffer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBuffer.class);
    public static final String LINE_SEPARATOR = "\n";
    private final StringJoiner buffer;

    public RecordBuffer() {
        super();
        buffer = new StringJoiner(LINE_SEPARATOR);
    }

    @Override
    public void insert(String record) {
        buffer.add(record);
        setNumOfRecords(getNumOfRecords() + 1);
        setBufferSizeBytes(getBufferSizeBytes() + record.getBytes(StandardCharsets.UTF_8).length);
    }

    public String getData() {
        String result = buffer.toString();
        LOG.debug(
            "flush buffer: {} records, {} bytes, offset {} - {}",
            getNumOfRecords(),
            getBufferSizeBytes(),
            getFirstOffset(),
            getLastOffset());
        return result;
    }
}
