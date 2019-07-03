package com.hazelcast.enterprise.wan.merkletree;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.function.Supplier;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreConsistent;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreInconsistent;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.removeFromMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.waitForReplicationToStart;
import static com.hazelcast.wan.fw.WanTestSupport.waitForSyncToComplete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanMerkleAntiEntropyTest {
    private static final String MAP_NAME = "MAP_WITH_MERKLETREES";
    private static final String REPLICATION_NAME = "wanReplication";
    private static final int MAP_ENTRIES = 100;

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Parameters(name = "inMemoryFormat: {0}")
    public static Collection<InMemoryFormat> parameters() {
        return asList(BINARY, OBJECT, NATIVE);
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void setup() {
        Supplier<Config> configSupplier = () -> new Config()
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(GroupProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(GroupProperty.EVENT_THREAD_COUNT.getName(), "1");

        sourceCluster = clusterA(factory, 2, configSupplier).setup();
        targetCluster = clusterB(factory, 2, configSupplier).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(MERKLE_TREES)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.getConfig()
                     .getMapConfig(MAP_NAME).getMerkleTreeConfig()
                     .setEnabled(true)
                     .setDepth(6);

        targetCluster.getConfig()
                     .getMapConfig(MAP_NAME).getMerkleTreeConfig()
                     .setEnabled(true)
                     .setDepth(6);

        sourceCluster.getConfig().getMapConfig(MAP_NAME)
                     .setInMemoryFormat(inMemoryFormat);
        targetCluster.getConfig().getMapConfig(MAP_NAME)
                     .setInMemoryFormat(inMemoryFormat);

        if (inMemoryFormat == NATIVE) {
            sourceCluster.getConfig().getNativeMemoryConfig()
                         .setAllocatorType(STANDARD)
                         .setEnabled(true);

            targetCluster.getConfig().getNativeMemoryConfig()
                         .setAllocatorType(STANDARD)
                         .setEnabled(true);
        }
    }

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Test
    public void testConsistencyCheckPutNotInSync() {
        givenTwoInconsistentClustersWithData();

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplication, MAP_NAME, MAP_ENTRIES);
    }

    @Test
    public void testConsistencyCheckPutAndRemoveAllInSync() {
        givenTwoInconsistentClustersWithData();
        removeFromMap(sourceCluster, MAP_NAME, 0, MAP_ENTRIES);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);
    }

    @Test
    public void testConsistencyCheckPutInSyncUpdateOneNotInSync() {
        givenTwoConsistentClustersWithData();

        IMap<Object, Object> map = sourceCluster.getAMember().getMap(MAP_NAME);
        map.replace(42, -1);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);
        int inconsistentPartitions = 1;
        int expectedEntriesToSync = 1;
        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplication, MAP_NAME, inconsistentPartitions,
                expectedEntriesToSync);
    }

    @Test
    public void testConsistencyCheckPutAndRemoveOneNotInSync() {
        givenTwoConsistentClustersWithData();

        IMap<Object, Object> map = sourceCluster.getAMember().getMap(MAP_NAME);
        map.remove(42);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);
        int inconsistentPartitions = 1;
        int expectedEntriesToSync = 0;
        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplication, MAP_NAME, inconsistentPartitions,
                expectedEntriesToSync);
    }

    @Test
    public void testConsistencyCheckAfterReplicationAllInSync() {
        givenTwoInconsistentClustersWithData(WanPublisherState.PAUSED);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);
        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplication, MAP_NAME, MAP_ENTRIES);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        waitForReplicationToStart(sourceCluster, targetCluster, wanReplication, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);
    }

    @Test
    public void testSyncMakesClustersConsistent() {
        givenTwoInconsistentClustersWithData();

        sourceCluster.syncMap(wanReplication, MAP_NAME);
        waitForSyncToComplete(sourceCluster);

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);
    }

    @Ignore("Fails if a source partition is empty, but the target is not")
    @Test
    public void testMerkleSyncRemovals() {
        givenTwoConsistentClustersWithData();

        IMap<Object, Object> sourceMap = sourceCluster.getAMember().getMap(MAP_NAME);
        IMap<Object, Object> targetMap = targetCluster.getAMember().getMap(MAP_NAME);
        sourceMap.remove(42);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        sourceCluster.syncMap(wanReplication, MAP_NAME);
        waitForSyncToComplete(sourceCluster);

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);

        assertEquals(sourceMap.size(), targetMap.size());
        assertEquals(MAP_ENTRIES - 1, sourceMap.size());
    }

    private void givenTwoConsistentClustersWithData() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        String valuePrefix = "T";
        fillMap(sourceCluster, MAP_NAME, 0, MAP_ENTRIES, valuePrefix);
        fillMap(targetCluster, MAP_NAME, 0, MAP_ENTRIES, valuePrefix);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);
        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);
    }

    private void givenTwoInconsistentClustersWithData() {
        givenTwoInconsistentClustersWithData(WanPublisherState.STOPPED);
    }

    private void givenTwoInconsistentClustersWithData(WanPublisherState initialWanReplicationState) {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        switch (initialWanReplicationState) {
            case STOPPED:
                sourceCluster.stopWanReplicationOnAllMembers(wanReplication);
                break;
            case PAUSED:
                sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
                break;
        }

        fillMap(sourceCluster, MAP_NAME, 0, MAP_ENTRIES);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);
        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplication, MAP_NAME, MAP_ENTRIES);
    }
}
