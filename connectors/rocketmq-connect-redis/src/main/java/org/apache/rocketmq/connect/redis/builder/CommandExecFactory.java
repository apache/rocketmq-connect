package org.apache.rocketmq.connect.redis.builder;

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
    }
    
    public static AbstractCommandExec findExec(final String command) {
        return COMMAND_BUILDER_MAP.get(command.toLowerCase());
    }

}
