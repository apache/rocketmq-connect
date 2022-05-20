package org.apache.rocketmq.connect.runtime.controller;

import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.rest.RestHandler;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * connect controller
 */
public abstract class AbstractConnectController implements ConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Configuration of current runtime.
     */
    protected  final ConnectConfig connectConfig;

    /**
     * All the configurations of current running connectors and tasks in cluster.
     */
    protected final ConfigManagementService configManagementService;

    /**
     * Position management of source tasks.
     */
    protected final PositionManagementService positionManagementService;

    /**
     * Offset management of sink tasks.
     */
    protected final PositionManagementService offsetManagementService;

    /**
     * Manage the online info of the cluster.
     */
    protected final ClusterManagementService clusterManagementService;

    /**
     * A worker to schedule all connectors and tasks assigned to current process.
     */
    protected final Worker worker;

    /**
     * A REST handler, interacting with user.
     */
    protected final RestHandler restHandler;

    /**
     * Thread pool to run schedule task.
     */
    protected static ScheduledExecutorService scheduledExecutorService;

    protected final Plugin plugin;

    protected final ConnectStatsManager connectStatsManager;

    protected final ConnectStatsService connectStatsService;

    static {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((Runnable r) -> new Thread(r, "ConnectScheduledThread"));
    }

    /**
     * init connect controller
     * @param connectConfig
     */
    public AbstractConnectController(
            Plugin plugin,
            ConnectConfig connectConfig,
            ClusterManagementService clusterManagementService,
            ConfigManagementService configManagementService,
            PositionManagementService positionManagementService,
            PositionManagementService offsetManagementService
    ) {
        // set config
        this.connectConfig = connectConfig;
        // set plugin
        this.plugin = plugin;
        // set metrics
        this.connectStatsManager = new ConnectStatsManager(connectConfig);
        this.connectStatsService = new ConnectStatsService();

        this.clusterManagementService = clusterManagementService;
        this.configManagementService = configManagementService;
        this.positionManagementService = positionManagementService;
        this.offsetManagementService = offsetManagementService;
        this.worker = new Worker(connectConfig, positionManagementService, configManagementService, plugin, this);
        this.restHandler = new RestHandler(this);
    }


    @Override
    public void start() {
        clusterManagementService.start();
        configManagementService.start();
        positionManagementService.start();
        offsetManagementService.start();
        worker.start();
        connectStatsService.start();

        // Persist configurations of current connectors and tasks.
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                this.configManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist config error.", e);
            }
        }, 1000, this.connectConfig.getConfigPersistInterval(), TimeUnit.MILLISECONDS);

        // Persist position information of source tasks.
        scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                this.positionManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist position error.", e);
            }

        }, 1000, this.connectConfig.getPositionPersistInterval(), TimeUnit.MILLISECONDS);

        // Persist offset information of sink tasks.
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                this.offsetManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist offset error.", e);
            }
        }, 1000, this.connectConfig.getOffsetPersistInterval(), TimeUnit.MILLISECONDS);

    }

    @Override
    public void shutdown() {

        if (worker != null) {
            worker.stop();
        }

        if (configManagementService != null) {
            configManagementService.stop();
        }

        if (positionManagementService != null) {
            positionManagementService.stop();
        }

        if (offsetManagementService != null) {
            offsetManagementService.stop();
        }

        if (clusterManagementService != null) {
            clusterManagementService.stop();
        }

        scheduledExecutorService.shutdown();
        try {
           scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown scheduledExecutorService error.", e);
        }
    }

    public ConnectConfig getConnectConfig() {
        return connectConfig;
    }

    public ConfigManagementService getConfigManagementService() {
        return configManagementService;
    }

    public PositionManagementService getPositionManagementService() {
        return positionManagementService;
    }

    public ClusterManagementService getClusterManagementService() {
        return clusterManagementService;
    }

    public Worker getWorker() {
        return worker;
    }

    public ConnectStatsManager getConnectStatsManager() {
        return connectStatsManager;
    }

    public ConnectStatsService getConnectStatsService() {
        return connectStatsService;
    }

}
