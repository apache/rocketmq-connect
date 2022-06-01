package org.apache.rocketmq.replicator;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.replicator.common.Utils;
import org.apache.rocketmq.replicator.config.ConfigDefine;
import org.apache.rocketmq.replicator.config.DataType;
import org.apache.rocketmq.replicator.config.RmqConnectorConfig;
import org.apache.rocketmq.replicator.config.TaskDivideConfig;
import org.apache.rocketmq.replicator.config.TaskTopicInfo;
import org.apache.rocketmq.replicator.strategy.DivideTaskByConsistentHash;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTaskDivideStrategyTest {

    @Test
    public void testDivideTaskByTopic() {
        RmqConnectorConfig config = new RmqConnectorConfig();
        KeyValue kv = new DefaultKeyValue();
        kv.put(ConfigDefine.CONN_TASK_DIVIDE_STRATEGY, "0");
        config.init(kv);
        TaskTopicInfo taskTopicInfo1 = new TaskTopicInfo("T_TEST1", "source_cluster", 0, "T_TEST1");
        TaskTopicInfo taskTopicInfo2 = new TaskTopicInfo("T_TEST2", "source_cluster", 0, "T_TEST2");
        TaskTopicInfo taskTopicInfo3 = new TaskTopicInfo("T_TEST3", "source_cluster", 0, "T_TEST3");
        TaskTopicInfo taskTopicInfo4 = new TaskTopicInfo("T_TEST4", "source_cluster", 0, "T_TEST4");
        TaskTopicInfo taskTopicInfo5 = new TaskTopicInfo("T_TEST5", "source_cluster", 0, "T_TEST5");
        Set<TaskTopicInfo> msgQueue1 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo1);
            }
        };
        Set<TaskTopicInfo> msgQueue2 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo2);
            }
        };
        Set<TaskTopicInfo> msgQueue3 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo3);
            }
        };
        Set<TaskTopicInfo> msgQueue4 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo4);
            }
        };
        Set<TaskTopicInfo> msgQueue5 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo5);
            }
        };
        Map<String, Set<TaskTopicInfo>> topicRouteMap = new HashMap<String, Set<TaskTopicInfo>>() {
            {
                put("T_TEST1", msgQueue1);
                put("T_TEST2", msgQueue2);
                put("T_TEST3", msgQueue3);
                put("T_TEST4", msgQueue4);
                put("T_TEST5", msgQueue5);

            }
        };
        TaskDivideConfig tdc = new TaskDivideConfig(
            "127.0.0.1:9876",
            "cluster_source",
            "store_topic",
            "",
            DataType.COMMON_MESSAGE.ordinal(),
            false,
            "",
            ""
        );
        List<KeyValue> taskConfigs = config.getTaskDivideStrategy().divide(topicRouteMap, tdc, 4);
        assertThat(taskConfigs.size()).isEqualTo(4);

        List<KeyValue> taskConfigs1 = config.getTaskDivideStrategy().divide(topicRouteMap, tdc, 6);
        assertThat(taskConfigs1.size()).isEqualTo(5);
    }

    @Test
    public void testDivideTaskByQueue() {
        RmqConnectorConfig config = new RmqConnectorConfig();
        KeyValue kv = new DefaultKeyValue();
        kv.put(ConfigDefine.CONN_TASK_DIVIDE_STRATEGY, "1");
        config.init(kv);
        TaskTopicInfo taskTopicInfo1 = new TaskTopicInfo("T_TEST1", "source_cluster", 0, "T_TEST1");
        TaskTopicInfo taskTopicInfo2 = new TaskTopicInfo("T_TEST1", "source_cluster", 1, "T_TEST1");
        TaskTopicInfo taskTopicInfo3 = new TaskTopicInfo("T_TEST1", "source_cluster", 2, "T_TEST1");
        TaskTopicInfo taskTopicInfo4 = new TaskTopicInfo("T_TEST2", "source_cluster", 0, "T_TEST2");
        TaskTopicInfo taskTopicInfo5 = new TaskTopicInfo("T_TEST2", "source_cluster", 1, "T_TEST2");
        Set<TaskTopicInfo> msgQueue1 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo1);
                add(taskTopicInfo2);
                add(taskTopicInfo3);
            }
        };
        Set<TaskTopicInfo> msgQueue2 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo4);
                add(taskTopicInfo5);
            }
        };
        Map<String, Set<TaskTopicInfo>> topicRouteMap = new HashMap<String, Set<TaskTopicInfo>>() {
            {
                put("T_TEST1", msgQueue1);
                put("T_TEST2", msgQueue2);
            }
        };
        TaskDivideConfig tdc = new TaskDivideConfig(
            "127.0.0.1:9876",
            "cluster_source",
            "store_topic",
            "",
            DataType.COMMON_MESSAGE.ordinal(),
            false,
            "",
            ""
        );
        List<KeyValue> taskConfigs = config.getTaskDivideStrategy().divide(topicRouteMap, tdc, 4);
        assertThat(taskConfigs.size()).isEqualTo(4);

        List<KeyValue> taskConfigs1 = config.getTaskDivideStrategy().divide(topicRouteMap, tdc, 6);
        assertThat(taskConfigs1.size()).isEqualTo(5);
    }

    @Test
    public void testDivideTaskByHash() {
        TaskTopicInfo taskTopicInfo1 = new TaskTopicInfo("T_TEST1", "source_cluster", 0, "T_TEST1");
        TaskTopicInfo taskTopicInfo2 = new TaskTopicInfo("T_TEST2", "source_cluster", 0, "T_TEST2");
        TaskTopicInfo taskTopicInfo3 = new TaskTopicInfo("T_TEST3", "source_cluster", 0, "T_TEST3");
        TaskTopicInfo taskTopicInfo4 = new TaskTopicInfo("T_TEST4", "source_cluster", 0, "T_TEST4");
        TaskTopicInfo taskTopicInfo5 = new TaskTopicInfo("T_TEST5", "source_cluster", 0, "T_TEST5");
        Set<TaskTopicInfo> msgQueue1 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo1);
            }
        };
        Set<TaskTopicInfo> msgQueue2 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo2);
            }
        };
        Set<TaskTopicInfo> msgQueue3 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo3);
            }
        };
        Set<TaskTopicInfo> msgQueue4 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo4);
            }
        };
        Set<TaskTopicInfo> msgQueue5 = new HashSet<TaskTopicInfo>() {
            {
                add(taskTopicInfo5);
            }
        };
        Map<String, Set<TaskTopicInfo>> topicRouteMap = new HashMap<String, Set<TaskTopicInfo>>() {
            {
                put("T_TEST1", msgQueue1);
                put("T_TEST2", msgQueue2);
                put("T_TEST3", msgQueue3);
                put("T_TEST4", msgQueue4);
                put("T_TEST5", msgQueue5);

            }
        };
        TaskDivideConfig tdc = new TaskDivideConfig(
            "127.0.0.1:9876",
            "cluster_source",
            "store_topic",
            "",
            DataType.COMMON_MESSAGE.ordinal(),
            false,
            "",
            ""
        );
        DivideTaskByConsistentHash hash = new DivideTaskByConsistentHash();
        List<KeyValue> taskConfigs = hash.divide(topicRouteMap, tdc, 3);
        assertThat(taskConfigs.size()).isEqualTo(3);

        List<KeyValue> taskConfigs1 = hash.divide(topicRouteMap, tdc, 6);
        assertThat(taskConfigs1.size()).isEqualTo(6);
    }

    @Test
    public void testDivideTaskByGroup() {
        RmqConnectorConfig config = new RmqConnectorConfig();
        KeyValue kv = new DefaultKeyValue();
        config.init(kv);
        List<String> knownGroups = new ArrayList<String>() {
            {
                add("G_TEST1");
                add("G_TEST2");
                add("G_TEST3");
                add("G_TEST4");
                add("G_TEST5");
            }
        };
        List<KeyValue> taskConfigs = Utils.groupPartitions(new ArrayList<>(knownGroups), config, 6);
        assertThat(taskConfigs.size()).isEqualTo(5);

        List<KeyValue> taskConfigs1 = Utils.groupPartitions(new ArrayList<>(knownGroups), config, 4);
        assertThat(taskConfigs1.size()).isEqualTo(4);
    }
}
