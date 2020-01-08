package com.hazelcast.client.management;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.management.MCWanBatchPublisherConfig;
import com.hazelcast.client.impl.management.ManagementCenterService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.DelegatingWanScheme;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE;
import static com.hazelcast.config.WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE;
import static com.hazelcast.wan.WanPublisherState.PAUSED;
import static com.hazelcast.wan.WanPublisherState.REPLICATING;
import static com.hazelcast.wan.WanPublisherState.STOPPED;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MCWanOperationsTest extends HazelcastTestSupport {
    private static final int NODE_COUNT = 3;
    private static final String WAN_REPLICATION_NAME = "wr-1";
    private static final String WAN_PUBLISHER_ID = "pub-1";
    private static final String MAP_NAME = "map-1";

    private Config targetClusterConfig = smallInstanceConfig().setClusterName("target-cluster");
    private TestHazelcastFactory factory;
    private ManagementCenterService managementCenterService;
    private HazelcastInstance[] hazelcastInstances = new HazelcastInstance[NODE_COUNT];
    private Member[] members;
    private HazelcastInstance targetInstance;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory(NODE_COUNT);

        targetInstance = factory.newHazelcastInstance(targetClusterConfig);

        Config sourceClusterConfig = sourceClusterConfig(
                targetInstance.getLocalEndpoint().getSocketAddress());
        hazelcastInstances = factory.newInstances(sourceClusterConfig);

        HazelcastInstance client = factory.newHazelcastClient();
        managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();

        members = stream(hazelcastInstances)
                .map(instance -> instance.getCluster().getLocalMember())
                .toArray(Member[]::new);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void changeWanReplicationState() throws Exception {
        resolve(managementCenterService.changeWanReplicationState(
                members[0], WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, STOPPED));
        assertPublisherState(hazelcastInstances[0], STOPPED);

        resolve(managementCenterService.changeWanReplicationState(
                members[1], WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, PAUSED));
        assertPublisherState(hazelcastInstances[1], PAUSED);

        resolve(managementCenterService.changeWanReplicationState(
                members[0], WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, REPLICATING));
        assertPublisherState(hazelcastInstances[0], REPLICATING);
    }

    @Test
    public void clearWanQueues() throws Exception {
        String key = generateKeyOwnedBy(hazelcastInstances[0]);

        pauseWanReplication(hazelcastInstances[0]);
        hazelcastInstances[0].getMap(MAP_NAME).put(key, 1);

        assertTrueEventually(() -> assertTrue(getOutboundQueueSize() > 0));

        resolve(managementCenterService.clearWanQueues(members[0], WAN_REPLICATION_NAME, WAN_PUBLISHER_ID));

        assertTrueEventually(() -> assertEquals(0, getOutboundQueueSize()));
    }

    private int getOutboundQueueSize() {
        return getNodeEngineImpl(hazelcastInstances[0])
                .getWanReplicationService()
                .getStats()
                .get(WAN_REPLICATION_NAME)
                .getLocalWanPublisherStats()
                .get(WAN_PUBLISHER_ID)
                .getOutboundQueueSize();
    }

    @Test
    public void addWanBatchPublisherConfig() throws Exception {
        String wanReplicationName = "dynamic-config";
        String publisherId = "pub-2";
        SocketAddress address = targetInstance.getLocalEndpoint().getSocketAddress();
        int queueCapacity = 28;
        int batchSize = 29;
        int batchMaxDelayMillis = 3_000;
        int responseTimeoutMillis = 4_000;
        WanAcknowledgeType ackType = ACK_ON_OPERATION_COMPLETE;
        WanQueueFullBehavior queueFullBehaviour = THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE;

        MCWanBatchPublisherConfig config = new MCWanBatchPublisherConfig();
        config.setName(wanReplicationName);
        config.setTargetCluster(targetClusterConfig.getClusterName());
        config.setPublisherId(publisherId);
        config.setEndpoints(addressAsString(address));
        config.setQueueCapacity(queueCapacity);
        config.setBatchSize(batchSize);
        config.setBatchMaxDelayMillis(batchMaxDelayMillis);
        config.setResponseTimeoutMillis(responseTimeoutMillis);
        config.setAckType(ackType);
        config.setQueueFullBehaviour(queueFullBehaviour);

        AddWanConfigResult result = resolve(managementCenterService.addWanReplicationConfig(config));
        assertEquals(publisherId, result.getAddedPublisherIds().iterator().next());
        assertTrue(result.getIgnoredPublisherIds().isEmpty());

        DelegatingWanScheme wanReplicationScheme = getNodeEngineImpl(hazelcastInstances[0])
                .getWanReplicationService().getWanReplicationPublishers(wanReplicationName);
        assertEquals(1, wanReplicationScheme.getPublishers().size());
        WanPublisher publisher = wanReplicationScheme.getPublisher(publisherId);
        assertNotNull(publisher);
        assertTrue(publisher instanceof WanBatchPublisher);
        WanBatchPublisher batchReplication = (WanBatchPublisher) publisher;
        assertEquals(1, batchReplication.getTargetEndpoints().size());
        assertEquals(address, batchReplication.getTargetEndpoints().get(0).getInetSocketAddress());
        assertEquals(batchSize, batchReplication.getConfigurationContext().getBatchSize());
        assertEquals(batchMaxDelayMillis, batchReplication.getConfigurationContext().getBatchMaxDelayMillis());
        assertEquals(responseTimeoutMillis, batchReplication.getConfigurationContext().getResponseTimeoutMillis());
        assertEquals(ackType, batchReplication.getConfigurationContext().getAcknowledgeType());

        WanBatchPublisherConfig publisherConfig =
                batchReplication.getConfigurationContext().getPublisherConfig();
        assertEquals(queueFullBehaviour, publisherConfig.getQueueFullBehavior());
        assertEquals(queueCapacity, publisherConfig.getQueueCapacity());
    }

    @Test
    public void wanSyncMap() throws Exception {
        pauseWanReplication(hazelcastInstances[0]);

        hazelcastInstances[0].getMap(MAP_NAME).put(3, 3);

        UUID uuid = resolve(managementCenterService.wanSyncMap(
                WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, MAP_NAME));
        assertNotNull(uuid);

        assertTrueEventually(() -> assertEquals(3, targetInstance.getMap(MAP_NAME).get(3)));
    }

    @Test
    public void wanSyncAllMaps() throws Exception {
        pauseWanReplication(hazelcastInstances[0]);

        hazelcastInstances[0].getMap(MAP_NAME).put(3, 3);

        UUID uuid = resolve(managementCenterService.wanSyncAllMaps(
                WAN_REPLICATION_NAME, WAN_PUBLISHER_ID));
        assertNotNull(uuid);

        assertTrueEventually(() -> assertEquals(3, targetInstance.getMap(MAP_NAME).get(3)));
    }

    @Test
    public void checkWanConsistency() throws Exception {
        UUID uuid = resolve(managementCenterService.checkWanConsistency(
                WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, MAP_NAME));
        assertNotNull(uuid);
    }

    @Test
    public void checkWanConsistency_invalidConfig() throws Exception {
        try {
            resolve(managementCenterService.checkWanConsistency(WAN_REPLICATION_NAME, "pub-9", MAP_NAME));
            fail("ExecutionException is expected to be thrown.");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof InvalidConfigurationException);
        }
    }

    private static <T> T resolve(CompletableFuture<T> future) throws Exception {
        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    private static void pauseWanReplication(HazelcastInstance instance) {
        getNodeEngineImpl(instance)
                .getWanReplicationService()
                .pause(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID);

        assertTrueEventually(() -> {
            WanBatchPublisher endpoint = (WanBatchPublisher) wanReplicationService(instance)
                    .getPublisherOrFail(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID);
            assertFalse(endpoint.getReplicationStrategy().hasOngoingReplication());
        });
    }

    private static void assertPublisherState(HazelcastInstance instance, WanPublisherState expectedState) {
        WanPublisherState publisherState = getNodeEngineImpl(instance)
                .getWanReplicationService()
                .getStats()
                .get(WAN_REPLICATION_NAME)
                .getLocalWanPublisherStats()
                .get(WAN_PUBLISHER_ID)
                .getPublisherState();

        assertEquals(expectedState, publisherState);
    }

    private Config sourceClusterConfig(SocketAddress socketAddress) {
        Config config = smallInstanceConfig();

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(WAN_REPLICATION_NAME);
        WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig();
        publisherConfig.setPublisherId(WAN_PUBLISHER_ID);
        publisherConfig.setClusterName(targetClusterConfig.getClusterName());
        publisherConfig.setTargetEndpoints(addressAsString(socketAddress));
        publisherConfig.setSyncConfig(
                new WanSyncConfig().setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES));
        wanReplicationConfig.addBatchReplicationPublisherConfig(publisherConfig);

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(WAN_REPLICATION_NAME);
        wanRef.setMergePolicyClassName(PassThroughMergePolicy.class.getName());
        wanRef.setRepublishingEnabled(false);

        MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
        merkleTreeConfig.setEnabled(true);

        config.getMapConfig(MAP_NAME)
                .setWanReplicationRef(wanRef)
                .setMerkleTreeConfig(merkleTreeConfig);

        config.addWanReplicationConfig(wanReplicationConfig);
        return config;
    }

    private String addressAsString(SocketAddress socketAddress) {
        InetSocketAddress address = (InetSocketAddress) socketAddress;
        return address.getAddress().getHostAddress() + ":" + address.getPort();
    }
}
