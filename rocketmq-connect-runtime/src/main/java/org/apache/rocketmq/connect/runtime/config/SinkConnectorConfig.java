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
 *
 */

package org.apache.rocketmq.connect.runtime.config;

import com.google.common.base.Splitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;

public class SinkConnectorConfig extends ConnectConfig {



    public static Map<String, String> parseTopicList(ConnectKeyValue taskConfig) {
        String topicNameAndTagss = taskConfig.getString(RuntimeConfigDefine.CONNECT_TOPICNAME);
        List<String> topicTagList = Splitter.on(COMMA).omitEmptyStrings().trimResults().splitToList(topicNameAndTagss);
        Map<String, String> topicNameAndTagssMap = new HashMap<>(8);
        for (String topicTagPair : topicTagList) {
            List<String> topicAndTag = Splitter.on(SEMICOLON).omitEmptyStrings().trimResults().splitToList(topicTagPair);
            if (topicAndTag.size() == 1) {
                topicNameAndTagssMap.put(topicAndTag.get(0), "*");
            } else {
                topicNameAndTagssMap.put(topicAndTag.get(0), topicAndTag.get(1));
            }
        }
        return topicNameAndTagssMap;
    }

    public static MessageQueue parseMessageQueueList(String messageQueueStr) {
        List<String> messageQueueStrList = Splitter.on(SEMICOLON).omitEmptyStrings().trimResults().splitToList(messageQueueStr);
        if (CollectionUtils.isEmpty(messageQueueStrList) || messageQueueStrList.size() != 3) {
            return null;
        }
        return new MessageQueue(messageQueueStrList.get(0), messageQueueStrList.get(1), Integer.valueOf(messageQueueStrList.get(2)));
    }
}
