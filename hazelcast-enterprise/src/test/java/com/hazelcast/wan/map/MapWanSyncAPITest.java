package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.sync.SyncFailedException;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapWanSyncAPITest extends MapWanReplicationTestSupport {

    @Parameterized.Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Parameterized.Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }


    @Override
    protected Config getConfig() {
        final Config config = super.getConfig();
        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat());

        if (consistencyCheckStrategy == MERKLE_TREES) {
            config.getMapMerkleTreeConfig("default")
                  .setEnabled(true)
                  .setDepth(6);
        }

        return config;
    }

    @Test
    public void basicSyncTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        wanReplicationService.syncMap("atob", "B", "map");

        assertKeysInEventually(clusterB, "map", 0, 1000);
    }

    @Test
    public void syncAllTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterA, "map2", 0, 2000);
        createDataIn(clusterA, "map3", 0, 3000);

        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);
        assertDataInFromEventually(clusterB, "map2", 0, 2000, clusterA);
        assertDataInFromEventually(clusterB, "map3", 0, 3000, clusterA);

        clusterB[0].getCluster().shutdown();
        startClusterB();

        assertKeysNotInEventually(clusterB, "map", 0, 1000);
        assertKeysNotInEventually(clusterB, "map2", 0, 2000);
        assertKeysNotInEventually(clusterB, "map3", 0, 3000);

        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        wanReplicationService.syncAllMaps("atob", getNode(clusterB).getConfig().getGroupConfig().getName());
        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map2", 0, 2000);
        assertKeysInEventually(clusterB, "map3", 0, 3000);
    }


    @Test(expected = SyncFailedException.class)
    public void sendMultipleSyncRequests() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        getWanReplicationService(clusterA[0]).syncMap("atob", configB.getGroupConfig().getName(), "map");
        getWanReplicationService(clusterA[0]).syncMap("atob", configB.getGroupConfig().getName(), "map");
    }
}
