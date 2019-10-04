package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.cluster.Address;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartClusterBasicTest extends AbstractHotRestartClusterStartTest {

    private int partitionCount = 271;
    private int partitionThreadCount = RuntimeAvailableProcessors.get();

    @Test
    public void testFreshStart() {
        HazelcastInstance[] instances = startNewInstances(4);
        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
        invokeDummyOperationOnAllPartitions(instances);
    }

    @Test
    public void test_hotRestart_withLegacyDir() {
        HazelcastInstance instance = startNewInstance();
        File hotRestartHome = getHotRestartHome(instance);
        terminateInstances();

        instance = startNewInstanceWithBaseDir(hotRestartHome);
        File hotRestartHome2 = getHotRestartHome(instance);

        assertEquals(hotRestartHome, hotRestartHome2);
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
    protected Config newConfig(ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        final Config config = super.newConfig(listener, clusterStartPolicy);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), String.valueOf(partitionThreadCount));
        return config;
    }
}
