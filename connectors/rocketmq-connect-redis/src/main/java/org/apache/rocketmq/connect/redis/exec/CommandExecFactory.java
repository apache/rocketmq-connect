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

package org.apache.rocketmq.connect.redis.exec;

import org.apache.rocketmq.connect.redis.parser.AbstractCommandParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol.Command;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class CommandExecFactory {
    
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommandParser.class);
    
    private static final Map<String, AbstractCommandExec> COMMAND_BUILDER_MAP = new LinkedHashMap<>();
    
    static {
        COMMAND_BUILDER_MAP.put(Command.SET.name().toLowerCase(), new SetExec());
        COMMAND_BUILDER_MAP.put(Command.APPEND.name().toLowerCase(), new AppendExec());
        COMMAND_BUILDER_MAP.put(Command.BITFIELD.name().toLowerCase(), new AppendExec());
    }
    
    public static AbstractCommandExec findExec(final String command) {
        return COMMAND_BUILDER_MAP.get(command.toLowerCase());
    }

}
