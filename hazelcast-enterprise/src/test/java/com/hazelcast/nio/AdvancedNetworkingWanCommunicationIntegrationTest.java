package com.hazelcast.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.networking.nio.AbstractAdvancedNetworkIntegrationTest;
import com.hazelcast.map.IMap;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkingWanCommunicationIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    private static final String CLUSTER_A_NAME = "Cluster-A";
    private static final String REPLICATED_MAP = "replicatedMap";

    @Test
    public void testWanConnectionToEndpoints() {
        Config config = createCompleteMultiSocketConfig();
        config.getGroupConfig().setName(CLUSTER_A_NAME);
        final HazelcastInstance hz = newHazelcastInstance(config);

        Config config2 = prepareWanAdvancedNetworkConfig(WAN1_PORT);
        HazelcastInstance hz2 = null;
        try {
            hz2 = Hazelcast.newHazelcastInstance(config2);
            IMap<String, String> map = hz2.getMap(REPLICATED_MAP);
            map.put("someKey", "someValue");

            assertEqualsEventually(new Callable<String>() {
                @Override
                public String call() {
                    IMap<String, String> map1 = hz.getMap(REPLICATED_MAP);
                    return map1.get("someKey");
                }
            }, "someValue");
        } finally {
            if (hz2 != null) {
                hz2.shutdown();
            }
        }

        testWanReplicationFailOnPort(hz, NOT_OPENED_PORT);
        testWanReplicationFailOnPort(hz, CLIENT_PORT);
    }

    private Config prepareWanAdvancedNetworkConfig(int port) {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig()
              .setEnabled(true)
              .addWanEndpointConfig(createServerSocketConfig(WAN1_PORT + 10, "WAN1"));
        addCommonWanReplication(config, port);
        return config;
    }

    private void testWanReplicationFailOnPort(HazelcastInstance hz, int port) {
        Config config = prepareWanAdvancedNetworkConfig(port);
        HazelcastInstance hz2 = null;
        try {
            hz2 = Hazelcast.newHazelcastInstance(config);
            IMap<String, String> map = hz2.getMap(REPLICATED_MAP);
            map.put("keyWhichIsNotReplicated", "someValueWhichIsNotReplicated");

            // we have to sleep here some time since we basically test that nothing was changed
            sleepSeconds(3);

            IMap<String, String> map1 = hz.getMap(REPLICATED_MAP);
            assertNull(map1.get("keyWhichIsNotReplicated"));
        } finally {
            if (hz2 != null) {
                hz2.shutdown();
            }
        }
    }

    private static void addCommonWanReplication(Config config, int port) {
        WanReplicationConfig wrConfig = new WanReplicationConfig();
        wrConfig.setName("my-wan-cluster");
        WanBatchReplicationPublisherConfig londonPublisherConfig = createWanPublisherConfig(
                CLUSTER_A_NAME,
                "127.0.0.1:" + port,
                ConsistencyCheckStrategy.NONE
        );
        wrConfig.addWanBatchReplicationPublisherConfig(londonPublisherConfig);

        config.addWanReplicationConfig(wrConfig);

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("my-wan-cluster");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        wanRef.setRepublishingEnabled(false);

        config.getMapConfig(REPLICATED_MAP).setWanReplicationRef(wanRef);
    }

    private static WanBatchReplicationPublisherConfig createWanPublisherConfig(String clusterName,
                                                                               String endpoints,
                                                                               ConsistencyCheckStrategy consistencyStrategy) {
        WanBatchReplicationPublisherConfig c = new WanBatchReplicationPublisherConfig();
        c.setGroupName(clusterName)
         .setQueueFullBehavior(WANQueueFullBehavior.DISCARD_AFTER_MUTATION)
         .setQueueCapacity(1000)
         .setBatchSize(500)
         .setBatchMaxDelayMillis(1000)
         .setSnapshotEnabled(false)
         .setResponseTimeoutMillis(60000)
         .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
         .setTargetEndpoints(endpoints)
         .setDiscoveryPeriodSeconds(20)
         .getWanSyncConfig().setConsistencyCheckStrategy(consistencyStrategy);
        return c;
    }

}
