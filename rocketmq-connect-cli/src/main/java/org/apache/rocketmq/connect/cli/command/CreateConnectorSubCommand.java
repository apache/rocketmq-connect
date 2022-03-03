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

package org.apache.rocketmq.connect.cli.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.connect.cli.commom.CLIConfigDefine;
import org.apache.rocketmq.connect.cli.commom.Config;
import org.apache.rocketmq.connect.cli.utils.RestSender;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class CreateConnectorSubCommand implements SubCommand {

    private final Config config;

    public CreateConnectorSubCommand(Config config) {
        this.config = config;
    }

    @Override
    public String commandName() {
        return "createConnector";
    }

    @Override
    public String commandDesc() {
        return "Create and start a connector by connector's config";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "connectorName", true, "connector name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("p", "path", true, "Configuration file path");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options) throws SubCommandException {
        try {
            String connectorName = commandLine.getOptionValue('c').trim();
            String request = "/connectors/" + connectorName + "?config=";
            URL baseUrl = new URL(CLIConfigDefine.PROTOCOL, config.getHttpAddr(), config.getHttpPort(), request);
            String filePath = commandLine.getOptionValue('p').trim();
            String config = readFile(filePath);
            String result = new RestSender().sendHttpRequest(baseUrl.toString(), config);
            System.out.printf(result + "%n");
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }

    private String readFile(String filePath) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));
            int index = 0;
            while ((index = reader.read()) != -1) {
                sb.append((char) index);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}
