package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

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
    public void tryToSyncNonExistingConfig() throws IOException {
        startClusterA();
        createDataIn(clusterA, "map", 0, 1000);
        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        String result = communicator.syncMapOverWAN("newWRConfigName", "groupName", "mapName");
        assertEquals("{\"status\":\"fail\",\"message\":\"WAN Replication Config doesn't exist with WAN configuration name newWRConfigName " +
                "and publisher target group name groupName\"}", result);
    }
}
