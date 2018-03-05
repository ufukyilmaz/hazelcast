package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterBasicTest extends AbstractHotRestartClusterStartTest {

    private int partitionCount = 271;
    private int partitionThreadCount = -1;
    private String explicitBaseDir = null;

    @Test
    public void testFreshStart() {
        HazelcastInstance[] instances = startNewInstances(4);
        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test(expected = HotRestartException.class)
    public void test_hotRestartFails_whenBaseDirUsed_byMultipleMembers() {
        explicitBaseDir = "hot-restart-dir";
        startNewInstance();
        startNewInstance();
    }

    @Test
    public void test_hotRestartFails_whenPartitionThreadCountIncreases() {
        partitionThreadCount = 4;

        HazelcastInstance hz = startNewInstance();
        Address address = getAddress(hz);
        terminateInstances();

        partitionThreadCount = 16;
        try {
            restartInstance(address);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
            ignore(expected);
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionThreadCountDecreases() {
        partitionThreadCount = 16;

        HazelcastInstance hz = startNewInstance();
        Address address = getAddress(hz);
        terminateInstances();

        partitionThreadCount = 4;
        try {
            restartInstance(address);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
            ignore(expected);
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionCountIncreased() {
        HazelcastInstance instance = startNewInstance();
        Address address = getAddress(instance);
        warmUpPartitions(instance);
        terminateInstances();

        partitionCount++;
        try {
            restartInstance(address);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
            ignore(expected);
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionCountDecreased() {
        HazelcastInstance instance = startNewInstance();
        Address address = getAddress(instance);
        warmUpPartitions(instance);
        terminateInstances();

        partitionCount--;
        try {
            restartInstance(address);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
            ignore(expected);
        }
    }

    @Override
    protected Config newConfig(String instanceName, ClusterHotRestartEventListener listener,
                               HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        final Config config = super.newConfig(instanceName, listener, clusterStartPolicy);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), String.valueOf(partitionThreadCount));

        if (explicitBaseDir != null) {
            config.getHotRestartPersistenceConfig().setBaseDir(new File(baseDir, explicitBaseDir));
        }

        return config;
    }
}
