package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.wan.WanSyncStatus;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import static com.hazelcast.wan.map.MapWanBatchReplicationTest.isAllMembersConnected;
import static com.hazelcast.wan.map.MapWanBatchReplicationTest.waitForSyncToComplete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MapWanSyncRESTTest extends MapWanReplicationTestSupport {

    @Parameterized.Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");

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
        final Config config = super.getConfig()
                                   .setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat());
        if (consistencyCheckStrategy == MERKLE_TREES) {
            config.getMapMerkleTreeConfig("default")
                  .setEnabled(true)
                  .setDepth(5);
        }
        return config;
    }

    @Test
    public void syncUsingRestApi() throws Exception {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        communicator.syncMapOverWAN("atob", "B", "map");

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA, "atob", "B")) {
            // we give another try to the sync if it failed because of the unsuccessful connection attempt
            communicator.syncMapOverWAN("atob", "B", "map");
        }

        assertKeysInEventually(clusterB, "map", 0, 1000);
    }

    @Test
    public void syncAllTestUsingREST() throws IOException {
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

        HTTPCommunicator communicator = new HTTPCommunicator(getNode(clusterA));
        communicator.syncMapsOverWAN("atob", getNode(clusterB).getConfig().getGroupConfig().getName());

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA, "atob", "B")) {
            // we give another try to the sync if it failed because of the unsuccessful connection attempt
            communicator.syncMapsOverWAN("atob", getNode(clusterB).getConfig().getGroupConfig().getName());
        }

        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map2", 0, 2000);
        assertKeysInEventually(clusterB, "map3", 0, 3000);
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
        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("newWRConfig");
        WanPublisherConfig newPublisherConfig = targetCluster(clusterB[0].getConfig(), clusterB.length);
        wanReplicationConfig.addWanPublisherConfig(newPublisherConfig);

        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanReplicationConfig);

        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        JsonObject jsonObject = toJsonObject(communicator.addWanConfig(dto.toJson().toString()));

        assertEquals("success", jsonObject.getString("status", null));
        assertEquals(1, jsonObject.get("addedPublisherIds").asArray().size());
        assertEquals(0, jsonObject.get("ignoredPublisherIds").asArray().size());

        createDataIn(clusterA, "map3", 0, 3000);
        assertKeysNotInEventually(clusterB, "map", 0, 1000);
        assertKeysNotInEventually(clusterB, "map2", 0, 2000);
        assertKeysNotInEventually(clusterB, "map3", 0, 3000);
        communicator = new HTTPCommunicator(clusterA[0]);
        jsonObject = toJsonObject(communicator.syncMapsOverWAN("newWRConfig", newPublisherConfig.getGroupName()));

        assertEquals("success", jsonObject.getString("status", null));

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA, "newWRConfig", newPublisherConfig.getGroupName())) {
            // we give another try to the sync if it failed because of the unsuccessful connection attempt
            communicator.syncMapsOverWAN("newWRConfig", newPublisherConfig.getGroupName());
        }

        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map2", 0, 2000);
        assertKeysInEventually(clusterB, "map3", 0, 3000);
    }

    @Test
    public void checkNewWanConfigExistsInNewNodesAndSyncTest() throws IOException {
        /* Form Cluster A by starting each instance with different Config object
        reference to prevent sharing */
        HazelcastInstance[] instance1 = new HazelcastInstance[1];
        HazelcastInstance[] instance2 = new HazelcastInstance[1];
        startClusterWithUniqueConfigObjects(instance1, configA);
        startClusterB();

        createDataIn(instance1, "map", 0, 1000);
        createDataIn(instance1, "map2", 0, 2000);
        sleepSeconds(5);
        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("newWRConfig");
        WanPublisherConfig newPublisherConfig = targetCluster(clusterB[0].getConfig(), clusterB.length);
        wanReplicationConfig.addWanPublisherConfig(newPublisherConfig);

        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanReplicationConfig);

        HTTPCommunicator communicator = new HTTPCommunicator(instance1[0]);
        JsonObject jsonObject = toJsonObject(communicator.addWanConfig(dto.toJson().toString()));

        assertEquals("success", jsonObject.getString("status", null));
        assertEquals(1, jsonObject.get("addedPublisherIds").asArray().size());
        assertEquals(0, jsonObject.get("ignoredPublisherIds").asArray().size());

        startClusterWithUniqueConfigObjects(instance2, configA.setInstanceName("confA-new"));
        assertClusterSizeEventually(2, instance1[0]);
        assert instance2[0].getConfig().getWanReplicationConfig("newWRConfig") != null;

        createDataIn(instance1, "map3", 0, 3000);
        assertKeysNotInEventually(clusterB, "map", 0, 1000);
        assertKeysNotInEventually(clusterB, "map2", 0, 2000);
        assertKeysNotInEventually(clusterB, "map3", 0, 3000);

        communicator = new HTTPCommunicator(instance2[0]);
        jsonObject = toJsonObject(communicator.syncMapsOverWAN("newWRConfig", newPublisherConfig.getGroupName()));

        assertEquals("success", jsonObject.getString("status", null));

        waitForSyncToComplete(instance2);
        if (!isAllMembersConnected(instance2, "newWRConfig", newPublisherConfig.getGroupName())) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            communicator.syncMapsOverWAN("newWRConfig", newPublisherConfig.getGroupName());
        }

        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map2", 0, 2000);
        assertKeysInEventually(clusterB, "map3", 0, 3000);
    }

    @Test
    public void tryToSyncNonExistingConfig() throws IOException {
        startClusterA();
        createDataIn(clusterA, "map", 0, 1000);
        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        JsonObject jsonObject = toJsonObject(communicator.syncMapOverWAN("newWRConfigName", "groupName", "mapName"));

        assertEquals("fail", jsonObject.getString("status", null));
        assertEquals("WAN Replication Config doesn't exist with WAN configuration "
                + "name newWRConfigName and publisher ID groupName", jsonObject.getString("message", null));
    }

    @Test
    public void sendMultipleSyncRequestsWithREST() throws IOException {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        HTTPCommunicator communicator = new HTTPCommunicator(clusterA[0]);
        JsonObject jsonObject;

        jsonObject = toJsonObject(communicator.syncMapsOverWAN("atob", configB.getGroupConfig().getName()));
        jsonObject = toJsonObject(communicator.syncMapsOverWAN("atob", configB.getGroupConfig().getName()));

        assertEquals("fail", jsonObject.getString("status", null));
        assertEquals("Another anti-entropy request is already in progress.", jsonObject.getString("message", null));
    }

    @Test
    public void checkWanSyncState() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                consistencyCheckStrategy);
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map", 0, 1000);

        clusterB[0].getCluster().shutdown();

        startClusterB();
        assertKeysNotInEventually(clusterB, "map", 0, 1000);

        final EnterpriseWanReplicationService service = getWanReplicationService(clusterA[0]);
        service.syncMap("atob", configB.getGroupConfig().getName(), "map");

        assertEquals(WanSyncStatus.IN_PROGRESS, service.getWanSyncState().getStatus());

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA, "atob", configB.getGroupConfig().getName())) {
            // we give another try to the sync if it failed because of the unsuccessful connection attempt
            service.syncMap("atob", configB.getGroupConfig().getName(), "map");
        }

        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertEqualsEventually(new Callable<WanSyncStatus>() {
            @Override
            public WanSyncStatus call() {
                return service.getWanSyncState().getStatus();
            }
        }, WanSyncStatus.READY);

        WanSyncState syncState = service.getWanSyncState();
        int member1SyncCount = syncState.getSyncedPartitionCount();
        int member2SyncCount = getWanReplicationService(clusterA[1]).getWanSyncState().getSyncedPartitionCount();
        int totalCount = getPartitionService(clusterA[0]).getPartitionCount();
        assertEquals(totalCount, member1SyncCount + member2SyncCount);
        assertEquals("atob", syncState.getActiveWanConfigName());
        assertEquals(configB.getGroupConfig().getName(), syncState.getActivePublisherName());
    }

    private void startClusterWithUniqueConfigObjects(HazelcastInstance[] cluster, Config config) {
        for (int i = 0; i < cluster.length; i++) {
            Config newConfig = getConfig();
            newConfig.getGroupConfig().setName(config.getGroupConfig().getName());
            newConfig.setInstanceName(config.getInstanceName() + 1);
            newConfig.getNetworkConfig().setPort(config.getNetworkConfig().getPort());
            newConfig.setInstanceName(config.getInstanceName() + i);
            cluster[i] = factory.newHazelcastInstance(newConfig);
        }
    }

    private JsonObject toJsonObject(String jsonStr) {
        JsonValue jsonValue = Json.parse(jsonStr);
        assertTrue(jsonValue.isObject());
        return jsonValue.asObject();
    }
}
