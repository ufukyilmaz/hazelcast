package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HotRestartClusterStartTest extends AbstractHotRestartClusterStartTest {

    private int partitionCount = 271;
    private int partitionThreadCount = -1;

    @Test
    public void testFreshStart() throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        final HazelcastInstance[] instances = startInstances(ports);

        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void test_hotRestart_afterClusterCrash_whenClusterState_ACTIVE() throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        startInstances(ports);
        assertInstancesJoined(4, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void test_hotRestart_afterClusterCrash_whenClusterState_FROZEN() throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        HazelcastInstance[] instances = startInstances(ports);

        assertInstancesJoined(ports.size(), instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);
        terminateInstances();

        instances = startInstances(ports);
        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.FROZEN);
    }

    @Test
    public void test_hotRestart_afterClusterCrash_whenClusterState_PASSIVE()
            throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        HazelcastInstance[] instances = startInstances(ports);

        assertInstancesJoined(4, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);
        terminateInstances();

        instances = startInstances(ports);
        assertInstancesJoined(4, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
    }

    @Test
    public void test_hotRestartFails_withMissingHotRestartDirectory_forOneNode()
            throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final Integer randomPort = ports.get(RandomPicker.getInt(ports.size()));
        deleteHotRestartDirectoryOfNode(randomPort);

        final HazelcastInstance[] instances = startInstances(ports);
        assertEquals(1, instances.length);

        HazelcastInstance instance = instances[0];
        assertTrue(getNode(instance).joined());
        assertClusterSizeEventually(1, instance);
    }

    @Test
    public void test_hotRestartFails_whenNodeStartsBeforeOthers_withMissingHotRestartDirectory()
            throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final Integer randomPort = ports.get(RandomPicker.getInt(ports.size()));
        deleteHotRestartDirectoryOfNode(randomPort);

        initializeFactory(ports);
        HazelcastInstance instance = startInstance(randomPort);
        assertTrue(getNode(instance).joined());

        ports.remove(randomPort);
        HazelcastInstance[] instances = startInstances(ports);
        assertEquals(0, instances.length);

        assertClusterSizeEventually(1, instance);
    }

    @Test
    public void test_hotRestartFails_withUnknownNode() throws IOException, InterruptedException {
        final List<Integer> ports = acquirePorts(4);
        startAndCrashInstances(ports);

        final List<Integer> portsWithUnknown = new ArrayList<Integer>(ports);
        int unknownPort = acquirePort();
        portsWithUnknown.add(unknownPort);
        initializeFactory(portsWithUnknown);
        HazelcastInstance unknownNode = startInstance(unknownPort);
        assertTrue(getNode(unknownNode).joined());

        final HazelcastInstance[] instances = startInstances(ports);
        assertEquals(0, instances.length);
    }

    private void deleteHotRestartDirectoryOfNode(int port) {
        final File[] perNodeHotRestartDirs = hotRestartDir.listFiles();
        if (perNodeHotRestartDirs != null) {
            for (File dir : perNodeHotRestartDirs) {
                if (dir.getName().equals("127.0.0.1-" + port)) {
                    IOUtil.delete(dir);
                    return;
                }
            }
            throw new IllegalArgumentException("no file is deleted!");
        } else {
            throw new IllegalStateException("no per node folders for hot restart");
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionThreadCountIncreases() {
        partitionThreadCount = 4;

        int port = acquirePort();
        initializeFactory(Collections.singletonList(port));
        startInstance(port);
        terminateInstances();

        partitionThreadCount = 128;
        initializeFactory(Collections.singletonList(port));
        try {
            startInstance(port);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionThreadCountDecreases() {
        partitionThreadCount = 128;

        int port = acquirePort();
        initializeFactory(Collections.singletonList(port));
        startInstance(port);
        terminateInstances();

        partitionThreadCount = 4;
        initializeFactory(Collections.singletonList(port));
        try {
            startInstance(port);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionCountIncreased() {
        int port = acquirePort();
        initializeFactory(Collections.singletonList(port));
        HazelcastInstance instance = startInstance(port);
        warmUpPartitions(instance);
        terminateInstances();

        partitionCount++;
        initializeFactory(Collections.singletonList(port));
        try {
            startInstance(port);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
        }
    }

    @Test
    public void test_hotRestartFails_whenPartitionCountDecreased() {
        int port = acquirePort();
        initializeFactory(Collections.singletonList(port));
        HazelcastInstance instance = startInstance(port);
        warmUpPartitions(instance);
        terminateInstances();

        partitionCount--;
        initializeFactory(Collections.singletonList(port));
        try {
            startInstance(port);
            fail("Hot restart should fail!");
        } catch (HotRestartException expected) {
        }
    }

    @Override
    protected Config newConfig(ClusterHotRestartEventListener listener) {
        final Config config = super.newConfig(listener);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), String.valueOf(partitionThreadCount));
        return config;
    }
}
