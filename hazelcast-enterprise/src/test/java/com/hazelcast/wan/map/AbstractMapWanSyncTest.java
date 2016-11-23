package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Base class to test WAN sync feature
 */
public abstract class AbstractMapWanSyncTest extends MapWanReplicationTestSupport {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setInMemoryFormat(getMemoryFormat());
        return config;
    }

    @Test
    public void basicSyncTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotIn(clusterB, "map", 0, 1000);

        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        wanReplicationService.syncMap("atob", "B", "map");

        assertKeysIn(clusterB, "map", 0, 1000);
    }

    @Test
    public void syncUsingRestApi() throws Exception {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotIn(clusterB, "map", 0, 1000);

        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        communicator.syncMapOverWAN("atob", "B", "map");

        assertKeysIn(clusterB, "map", 0, 1000);
    }

    @Test
    public void syncAllTest() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterA, "map2", 0, 2000);
        createDataIn(clusterA, "map3", 0, 3000);

        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterB, "map2", 0, 2000, clusterA);
        assertDataInFrom(clusterB, "map3", 0, 3000, clusterA);

        clusterB[0].getCluster().shutdown();
        startClusterB();

        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterB, "map2", 0, 2000);
        assertKeysNotIn(clusterB, "map3", 0, 3000);

        EnterpriseWanReplicationService wanReplicationService
                = (EnterpriseWanReplicationService) getNode(clusterA[0]).nodeEngine.getWanReplicationService();
        wanReplicationService.syncAllMaps("atob", getNode(clusterB).getConfig().getGroupConfig().getName());
        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterB, "map2", 0, 2000);
        assertKeysIn(clusterB, "map3", 0, 3000);
    }

    @Test
    public void syncTestUsingREST() throws IOException {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterA, "map2", 0, 2000);
        createDataIn(clusterA, "map3", 0, 3000);

        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterB, "map2", 0, 2000, clusterA);
        assertDataInFrom(clusterB, "map3", 0, 3000, clusterA);

        clusterB[0].getCluster().shutdown();
        startClusterB();

        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterB, "map2", 0, 2000);
        assertKeysNotIn(clusterB, "map3", 0, 3000);

        HTTPCommunicator communicator = new HTTPCommunicator(getNode(clusterA));
        communicator.syncMapsOverWAN("atob", getNode(clusterB).getConfig().getGroupConfig().getName());
        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterB, "map2", 0, 2000);
        assertKeysIn(clusterB, "map3", 0, 3000);
    }
}
