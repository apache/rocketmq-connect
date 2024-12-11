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

package org.apache.rocketmq.connect.doris.metrics;

public class MetricsUtil {
    public static final String JMX_METRIC_PREFIX = "kafka.connector.doris";

    // Offset related constants
    public static final String TOTAL_PROCESSED_DOMAIN = "total-processed";

    // total number of data successfully imported to doris through stream-load (or the total number
    // of data files uploaded through copy-into).
    public static final String TOTAL_LOAD_COUNT = "total-load-count";
    public static final String TOTAL_RECORD_COUNT = "total-record-count";
    public static final String TOTAL_DATA_SIZE = "total-data-size";

    // file count related constants
    public static final String OFFSET_DOMAIN = "offsets";
    // Successfully submitted data to doris' offset
    public static final String COMMITTED_OFFSET = "committed-offset";

    // Buffer related constants
    public static final String BUFFER_DOMAIN = "buffer";
    public static final String BUFFER_MEMORY_USAGE = "buffer-memory-usage";
    public static final String BUFFER_SIZE_BYTES = "buffer-size-bytes";
    public static final String BUFFER_RECORD_COUNT = "buffer-record-count";

    public static String constructMetricName(
        final Integer taskId, final String domain, final String metricName) {
        return String.format("%s/%s/%s", taskId, domain, metricName);
    }
}
