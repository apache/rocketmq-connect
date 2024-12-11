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

package org.apache.rocketmq.connect.doris.utils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.model.BackendV2;
import org.apache.rocketmq.connect.doris.service.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BackendUtils.class);
    private final List<BackendV2.BackendRowV2> backends;
    private long pos;

    public BackendUtils(List<BackendV2.BackendRowV2> backends) {
        this.backends = backends;
        this.pos = 0;
    }

    public static BackendUtils getInstance(DorisOptions dorisOptions, Logger logger) {
        return new BackendUtils(RestService.getBackendsV2(dorisOptions, logger));
    }

    public String getAvailableBackend() {
        long tmp = pos + backends.size();
        while (pos < tmp) {
            BackendV2.BackendRowV2 backend = backends.get((int) (pos++ % backends.size()));
            String res = backend.toBackendString();
            if (tryHttpConnection(res)) {
                return res;
            }
        }
        throw new DorisException("no available backend.");
    }

    public static boolean tryHttpConnection(String backend) {
        try {
            backend = "http://" + backend;
            URL url = new URL(backend);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(60000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to backend:{}", backend, ex);
            return false;
        }
    }
}
