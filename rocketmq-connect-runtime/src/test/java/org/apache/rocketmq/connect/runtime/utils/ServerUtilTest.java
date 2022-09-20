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

package org.apache.rocketmq.connect.runtime.utils;

import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class ServerUtilTest {

    private static final String APP_NAME = "testAppName";

    @Test
    public void buildCommandlineOptionsTest() {
        Options options = new Options();
        Option opt = new Option("n", "nameserver", true, "nameServer address");
        opt.setRequired(true);
        options.addOption(opt);
        final Options options1 = ServerUtil.buildCommandlineOptions(options);
        Assert.assertEquals("n", options1.getRequiredOptions().get(0));
    }

    @Test
    public void parseCmdLineTest() {
        Options options = new Options();
        Option opt = new Option("n", "nameserver", true, "nameServer address");
        opt.setRequired(true);
        options.addOption(opt);
        String[] args = new String[]{"-n localhost:9876"};
        final CommandLine commandLine = ServerUtil.parseCmdLine(APP_NAME, args, options, new PosixParser());
        Assert.assertEquals(" localhost:9876", commandLine.getOptionValue("n"));
    }

    @Test
    public void printCommandLineHelpTest() {
        Options options = new Options();
        Option opt = new Option("n", "nameserver", true, "nameServer address");
        opt.setRequired(true);
        options.addOption(opt);
        Assertions.assertThatCode(() -> ServerUtil.printCommandLineHelp(APP_NAME, options)).doesNotThrowAnyException();
    }

    @Test
    public void commandLine2PropertiesTest() {
        Options options = new Options();
        Option opt = new Option("n", "nameServer", true, "NameServer address");
        opt.setRequired(true);
        options.addOption(opt);
        String[] args = new String[]{"-n localhost:9876"};
        final CommandLine commandLine = ServerUtil.parseCmdLine(APP_NAME, args, options, new PosixParser());
        final Properties properties = ServerUtil.commandLine2Properties(commandLine);
        Assert.assertEquals(" localhost:9876", properties.get("nameServer"));
    }
}
