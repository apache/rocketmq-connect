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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConfig;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConnectController;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.service.memory.FilePositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryStateManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.utils.FileAndPropertyUtil;
import org.apache.rocketmq.connect.runtime.utils.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Startup class of the runtime worker.
 */
public class StandaloneConnectStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    public static CommandLine commandLine = null;

    public static String configFile = null;

    public static Properties properties = null;

    public static void main(String[] args) {
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
            StandaloneConfig config = new StandaloneConfig();
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c').trim();
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    FileAndPropertyUtil.properties2Object(properties, config);
                    in.close();
                }
            }

            if (StringUtils.isNotEmpty(config.getMetricsConfigPath())) {
                String file = config.getMetricsConfigPath();
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                Map<String, String> metricsConfig = new ConcurrentHashMap<>();
                if (properties.contains(WorkerConfig.METRIC_CLASS)) {
                    throw new IllegalArgumentException("[metrics.reporter] is empty");
                }
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    if (entry.getKey().equals(WorkerConfig.METRIC_CLASS)) {
                        continue;
                    }
                    metricsConfig.put(entry.getKey().toString(), entry.getValue().toString());
                }
                config.getMetricsConfig().put(properties.getProperty(WorkerConfig.METRIC_CLASS), metricsConfig);
                in.close();
            }

            if (null == config.getConnectHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the Connect installation", WorkerConfig.CONNECT_HOME_ENV);
                System.exit(-2);
            }

            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(config.getConnectHome() + "/conf/logback.xml");

            List<String> pluginPaths = new ArrayList<>(16);
            if (StringUtils.isNotEmpty(config.getPluginPaths())) {
                String[] strArr = config.getPluginPaths().split(",");
                for (String path : strArr) {
                    if (StringUtils.isNotEmpty(path)) {
                        pluginPaths.add(path);
                    }
                }
            }
            Plugin plugin = new Plugin(pluginPaths);
            ClusterManagementService clusterManagementService = new MemoryClusterManagementServiceImpl();
            clusterManagementService.initialize(config);
            ConfigManagementService configManagementService = new MemoryConfigManagementServiceImpl();
            configManagementService.initialize(config, null, plugin);
            PositionManagementService positionManagementServices = new FilePositionManagementServiceImpl();
            positionManagementServices.initialize(config, null, null);
            StateManagementService stateManagementService = new MemoryStateManagementServiceImpl();
            stateManagementService.initialize(config, null);
            StandaloneConnectController controller = new StandaloneConnectController(
                    plugin,
                    config,
                    clusterManagementService,
                    configManagementService,
                    positionManagementServices,
                    stateManagementService);
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
