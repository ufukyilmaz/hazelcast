package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
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
    public void syncAllTestUsingREST() throws IOException {
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

    @Test
    public void addNewWanConfigAndSyncTest() throws IOException {
        /*Form Cluster A by starting each instance with different Config object
        reference to prevent sharing */
        startClusterWithUniqueConfigObjects(clusterA, configA);
        startClusterB();
        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterA, "map2", 0, 2000);
        sleepSeconds(5);
        assertKeysNotIn(clusterB, "map", 0, 1000);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("newWRConfig");
        WanPublisherConfig newPublisherConfig = targetCluster(clusterB[0].getConfig(), clusterB.length);
        wanReplicationConfig.addWanPublisherConfig(newPublisherConfig);

        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanReplicationConfig);

        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        communicator.addWanConfig(dto.toJson().toString());

        createDataIn(clusterA, "map3", 0, 3000);
        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterB, "map2", 0, 2000);
        assertKeysNotIn(clusterB, "map3", 0, 3000);
        communicator = new HTTPCommunicator(clusterA[0]);
        communicator.syncMapsOverWAN("newWRConfig", newPublisherConfig.getGroupName());
        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterB, "map2", 0, 2000);
        assertKeysIn(clusterB, "map3", 0, 3000);
    }

    @Test
    public void checkNewWanConfigExistsInNewNodesAndSyncTest() throws IOException {
        /*Form Cluster A by starting each instance with different Config object
        reference to prevent sharing */
        HazelcastInstance[] instance1 = new HazelcastInstance[1];
        HazelcastInstance[] instance2 = new HazelcastInstance[1];
        startClusterWithUniqueConfigObjects(instance1, configA);
        startClusterB();

        createDataIn(instance1, "map", 0, 1000);
        createDataIn(instance1, "map2", 0, 2000);
        sleepSeconds(5);
        assertKeysNotIn(clusterB, "map", 0, 1000);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("newWRConfig");
        WanPublisherConfig newPublisherConfig = targetCluster(clusterB[0].getConfig(), clusterB.length);
        wanReplicationConfig.addWanPublisherConfig(newPublisherConfig);

        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanReplicationConfig);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1[0]);
        communicator.addWanConfig(dto.toJson().toString());

        startClusterWithUniqueConfigObjects(instance2, configA.setInstanceName("confA-new"));
        assertClusterSizeEventually(2, instance1[0]);
        assert instance2[0].getConfig().getWanReplicationConfig("newWRConfig") != null;

        createDataIn(instance1, "map3", 0, 3000);
        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterB, "map2", 0, 2000);
        assertKeysNotIn(clusterB, "map3", 0, 3000);

        communicator = new HTTPCommunicator(instance2[0]);
        communicator.syncMapsOverWAN("newWRConfig", newPublisherConfig.getGroupName());

        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterB, "map2", 0, 2000);
        assertKeysIn(clusterB, "map3", 0, 3000);
    }

    @Test
    public void tryToSyncNonExistingConfig() throws IOException {
        startClusterA();
        createDataIn(clusterA, "map", 0, 1000);
        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        String result = communicator.syncMapOverWAN("newWRConfigName", "groupName", "mapName");
        assertEquals("{\"status\":\"fail\",\"message\":\"WAN Replication Config doesn't exists with WAN configuration name newWRConfigName " +
                "and publisher target group name groupName\"}", result);
    }

    private void startClusterWithUniqueConfigObjects(HazelcastInstance[] cluster, Config config) {
        for (int i = 0; i < cluster.length; i++) {
            Config newConfig = getConfig();
            newConfig.getGroupConfig().setName(config.getGroupConfig().getName());
            newConfig.setInstanceName(config.getInstanceName() + 1);
            newConfig.getNetworkConfig().setPort(config.getNetworkConfig().getPort());
            newConfig.setInstanceName(config.getInstanceName() + i);
            cluster[i] = HazelcastInstanceFactory.newHazelcastInstance(newConfig);
        }
    }
}
