package com.hazelcast.enterprise.wan.merkletree;

import com.hazelcast.config.InMemoryFormat;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.config.WanPublisherState.STOPPED;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreConsistent;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreInconsistent;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanMerkleConsistencyCheckTest {
    private static final String MAP_NAME = "MAP_WITH_MERKLETREES";
    private static final String REPLICATION_NAME = "wanReplication";

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Parameters(name = "inMemoryFormat: {0} sourceDepth:{1} targetDepth:{2} partitions:{3} elements:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{

                // the cases below mostly work with very small record counts
                // this is because with this approach we can better approximate the expected partition difference count to the
                // real count

                // case: only one cluster member holds records to compare (no error on the other member)
                {OBJECT, 3, 3, 3, 1},
                {NATIVE, 3, 3, 3, 1},

                // case: higher record than partition count
                {OBJECT, 5, 5, 271, 1000},
                {NATIVE, 5, 5, 271, 1000},

                // the cases below test that:
                // - source and target Merkle trees have the same depth
                // - source Merkle trees are deeper than target trees
                // - source Merkle trees are shallower than target trees

                // case: even depth with smaller record than partition count
                {OBJECT, 3, 4, 5, 2},
                {OBJECT, 4, 4, 5, 2},
                {OBJECT, 4, 3, 5, 2},
                {NATIVE, 3, 4, 5, 2},
                {NATIVE, 4, 4, 5, 2},
                {NATIVE, 4, 3, 5, 2},

                // case: odd depth with smaller record than partition count
                {OBJECT, 5, 8, 7, 3},
                {OBJECT, 5, 5, 7, 3},
                {OBJECT, 8, 5, 7, 3},
                {NATIVE, 5, 8, 7, 3},
                {NATIVE, 5, 5, 7, 3},
                {NATIVE, 8, 5, 7, 3},

                // case: odd depths with highly unbalanced tree depths; smaller record than partition count
                {OBJECT, 5, 11, 271, 100},
                {OBJECT, 11, 11, 271, 100},
                {OBJECT, 11, 5, 271, 100},
                {NATIVE, 5, 11, 271, 100},
                {NATIVE, 11, 11, 271, 100},
                {NATIVE, 11, 5, 271, 100},
                });
    }

    @Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public int sourceTreeDepth;

    @Parameter(2)
    public int targetTreeDepth;

    @Parameter(3)
    public int partitions;

    @Parameter(4)
    public int entriesToPut;

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 2).setup();
        targetCluster = clusterB(factory, 2).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(MERKLE_TREES)
                .withInitialPublisherState(STOPPED)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.getConfig()
                     .getMapConfig(MAP_NAME).getMerkleTreeConfig()
                     .setEnabled(true)
                     .setDepth(sourceTreeDepth);

        targetCluster.getConfig()
                     .getMapConfig(MAP_NAME).getMerkleTreeConfig()
                     .setEnabled(true)
                     .setDepth(targetTreeDepth);

        sourceCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitions));
        targetCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(partitions));

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

    @Test
    public void testConsistencyCheckDifferences() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut);

        assertClusterSizeEventually(2, sourceCluster.getMembers());
        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreInconsistent(sourceCluster, wanReplication, MAP_NAME, entriesToPut);
    }

    @Test
    public void testConsistencyCheckAllInSync() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        String valuePrefixCommonOnBothClusters = "T";
        fillMap(sourceCluster, MAP_NAME, 0, entriesToPut, valuePrefixCommonOnBothClusters);
        fillMap(targetCluster, MAP_NAME, 0, entriesToPut, valuePrefixCommonOnBothClusters);

        assertClusterSizeEventually(2, sourceCluster.getMembers());
        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);
    }
}
