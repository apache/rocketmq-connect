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
package org.apache.rocketmq.replicator.utils;

import io.openmessaging.KeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.replicator.exception.ParamInvalidException;

import java.util.*;


/**
 * @author osgoo
 * @date 2022/7/21
 */
public class ReplicatorUtils {
    private static Log log = LogFactory.getLog(ReplicatorUtils.class);
    public static String buildTopicWithNamespace(String rawTopic, String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return rawTopic;
        }
        return instanceId + "%" + rawTopic;
    }

    public static void checkNeedParams(String connectorName, KeyValue config, Set<String> neededParamKeys) {
        for (String needParamKey : neededParamKeys) {
            checkNeedParamNotEmpty(connectorName, config, needParamKey);
        }
    }

    public static void checkNeedParamNotEmpty(String connectorName, KeyValue config, String needParamKey) {
        if (StringUtils.isEmpty(config.getString(needParamKey, ""))) {
            log.error("Replicator connector " + connectorName + " do not set " + needParamKey);
            throw new ParamInvalidException("Replicator connector " + connectorName + " do not set " + needParamKey);
        }
    }

    public static List sortList(List configs, Comparator comparable) {
        Object[] sortedKv = configs.toArray();
        Arrays.sort(sortedKv, comparable);
        configs = Arrays.asList(sortedKv);
        return configs;
    }
}
