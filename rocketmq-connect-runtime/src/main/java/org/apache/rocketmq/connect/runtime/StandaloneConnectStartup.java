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

package org.apache.rocketmq.connect.runtime;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConfig;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConnectController;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StagingMode;
import org.apache.rocketmq.connect.runtime.utils.FileAndPropertyUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.connect.runtime.utils.ServerUtil;
import org.apache.rocketmq.connect.runtime.utils.ServiceProviderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup class of the runtime worker.
 */
public class StandaloneConnectStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    public static CommandLine commandLine = null;

    public static String configFile = null;

    public static Properties properties = null;

    public static void main(String[] args) {
        args =new String[]{"-c /Users/sunxiaojian/rocketmq-connect/distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT/conf/connect-standalone.conf"};
        start(createConnectController(args));
    }

    private static void start(StandaloneConnectController controller) {

        try {
            controller.start();
            String tip = "The standalone worker boot success.";
            log.info(tip);
            System.out.printf("%s%n", tip);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Read configs from command line and create connect controller.
     *
     * @param args
     * @return
     */
    private static StandaloneConnectController createConnectController(String[] args) {
        try {
            // Build the command line options.
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("connect", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            // Load configs from command line.
            StandaloneConfig connectConfig = new StandaloneConfig();
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c').trim();
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    FileAndPropertyUtil.properties2Object(properties, connectConfig);
                    in.close();
                }
            }

//            if (null == connectConfig.getConnectHome()) {
//                System.out.printf("Please set the %s variable in your environment to match the location of the Connect installation", ConnectConfig.CONNECT_HOME_ENV);
//                System.exit(-2);
//            }
//
//            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
//            JoranConfigurator configurator = new JoranConfigurator();
//            configurator.setContext(lc);
//            lc.reset();
//            configurator.doConfigure(connectConfig.getConnectHome() + "/conf/logback.xml");

            List<String> pluginPaths = new ArrayList<>(16);
            if (StringUtils.isNotEmpty(connectConfig.getPluginPaths())) {
                String[] strArr = connectConfig.getPluginPaths().split(",");
                for (String path : strArr) {
                    if (StringUtils.isNotEmpty(path)) {
                        pluginPaths.add(path);
                    }
                }
            }
            Plugin plugin = new Plugin(pluginPaths);
            plugin.initPlugin();

            ClusterManagementService clusterManagementService = ServiceProviderUtil.getClusterManagementServices(StagingMode.STANDALONE);
            clusterManagementService.initialize(connectConfig);
            ConfigManagementService configManagementService = ServiceProviderUtil.getConfigManagementServices(StagingMode.STANDALONE);
            configManagementService.initialize(connectConfig, plugin);
            PositionManagementService positionManagementServices = ServiceProviderUtil.getPositionManagementServices(StagingMode.STANDALONE);
            positionManagementServices.initialize(connectConfig);

            StandaloneConnectController controller = new StandaloneConnectController(
                plugin,
                connectConfig,
                clusterManagementService,
                configManagementService,
                positionManagementServices);
            // Invoked when shutdown.
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));
            return controller;

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static Options buildCommandlineOptions(Options options) {

        Option opt = new Option("c", "configFile", true, "connect config properties file");
        opt.setRequired(false);
        options.addOption(opt);
        return options;

    }
}
