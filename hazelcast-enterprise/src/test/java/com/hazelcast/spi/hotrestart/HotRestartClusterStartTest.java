package com.hazelcast.spi.hotrestart;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HotRestartClusterStartTest extends AbstractHotRestartClusterStartTest {

    @Test
    public void testFreshStart() throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        final HazelcastInstance[] instances = startInstances(ports, ports);

        assertInstancesJoined(ports, instances, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void test_hotRestart_afterClusterCrash_whenClusterState_ACTIVE() throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        startAndCrashInstances(ports);

        startInstances(ports, ports);
        assertInstancesJoined(ports, NodeState.ACTIVE, ClusterState.ACTIVE);
    }

    @Test
    public void test_hotRestart_afterClusterCrash_whenClusterState_FROZEN() throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        HazelcastInstance[] instances = startInstances(ports, ports);

        assertInstancesJoined(ports, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.FROZEN);
        HazelcastInstanceFactory.terminateAll();

        instances = startInstances(ports, ports);
        assertInstancesJoined(ports, instances, NodeState.ACTIVE, ClusterState.FROZEN);
    }

    @Test
    public void test_hotRestart_afterClusterShutdown_whenClusterState_ACTIVE()
            throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        HazelcastInstance[] instances = startInstances(ports, ports);

        assertInstancesJoined(ports, instances, NodeState.ACTIVE, ClusterState.ACTIVE);

        warmUpPartitions(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);
        HazelcastInstanceFactory.terminateAll();

        instances = startInstances(ports, ports);
        assertInstancesJoined(ports, instances, NodeState.PASSIVE, ClusterState.PASSIVE);
    }

    @Test
    public void test_hotRestartFails_withMissingHotRestartDirectory_forOneNode()
            throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        startAndCrashInstances(ports);

        final Integer randomPort = ports.get(RandomPicker.getInt(ports.size()));
        deleteHotRestartDirectoryOfNode(randomPort);

        final HazelcastInstance[] instances = startInstances(ports, ports);
        assertEquals(1, instances.length);

        HazelcastInstance instance = instances[0];
        assertTrue(getNode(instance).joined());
        assertClusterSizeEventually(1, instance);
    }

    @Test
    public void test_hotRestartFails_whenNodeStartsBeforeOthers_withMissingHotRestartDirectory()
            throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        startAndCrashInstances(ports);

        final Integer randomPort = ports.get(RandomPicker.getInt(ports.size()));
        deleteHotRestartDirectoryOfNode(randomPort);

        HazelcastInstance[] instances = startInstances(Collections.singletonList(randomPort), ports);
        assertEquals(1, instances.length);
        HazelcastInstance instance = instances[0];
        assertTrue(getNode(instance).joined());

        instances = startInstances(ports.subList(1, ports.size()), ports);
        assertEquals(0, instances.length);

        assertClusterSizeEventually(1, instance);
    }

    @Test
    public void test_hotRestartFails_withUnknownNode() throws IOException, InterruptedException {
        final List<Integer> ports = Arrays.asList(5701, 5702, 5703, 5704);
        startAndCrashInstances(ports);

        final List<Integer> portsWithUnknown = Arrays.asList(5701, 5702, 5703, 5704, 5705);
        HazelcastInstance[] unknownNodes = startInstances(Collections.singletonList(5705), portsWithUnknown);
        assertEquals(1, unknownNodes.length);

        final HazelcastInstance[] instances = startInstances(ports, portsWithUnknown);
        assertEquals(0, instances.length);
    }

    private void deleteHotRestartDirectoryOfNode(int port) {
        final File hotRestartDir = new File(HOT_RESTART_DIRECTORY);
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

}
