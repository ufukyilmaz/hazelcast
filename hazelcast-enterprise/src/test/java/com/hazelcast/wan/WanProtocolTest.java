package com.hazelcast.wan;

import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationResponse;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.wan.WanReplicationTestSupport.getWanReplicationService;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanProtocolTest extends HazelcastTestSupport {

    private static final String WAN_REPLICATION_SCHEME = "abc";
    private static final String MAP_NAME = "dummyMap";

    private TestHazelcastInstanceFactory factory;
    private Cluster clusterA;
    private Cluster clusterB;
    private WanReplication wanReplication;

    @Before
    public void initClusters() {
        factory = new CustomNodeExtensionTestInstanceFactory(
                node -> new WanServiceMockingEnterpriseNodeExtension(node, spy(new EnterpriseWanReplicationService(node))));
        clusterA = clusterA(factory, 2).setup();
        clusterB = clusterB(factory, 2).setup();

        wanReplication = replicate()
                .from(clusterA)
                .to(clusterB)
                .withSetupName(WAN_REPLICATION_SCHEME)
                .setup();

        clusterA.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();
    }

    @After
    public void cleanup() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void testLatestWanProtocolIsSelected() {
        clusterA.startCluster();
        clusterB.startCluster();

        setSupportedWanProtocolVersions(clusterA, Version.of(1, 0), Version.of(1, 1), Version.of(1, 2));
        setSupportedWanProtocolVersions(clusterB, Version.of(1, 2));

        fillMap(clusterA, MAP_NAME, 0, 10);
        verifyMapReplicated(clusterA, clusterB, MAP_NAME);
        assertNegotiatedWanProtocolVersion(clusterA, wanReplication, Version.of(1, 2));
    }

    @Test
    public void testNoMatchingProtocol() {
        clusterA.startCluster();
        clusterB.startCluster();

        setSupportedWanProtocolVersions(clusterA, Version.of(1, 0), Version.of(1, 1));
        setSupportedWanProtocolVersions(clusterB, Version.of(1, 2));

        fillMap(clusterA, MAP_NAME, 0, 10);
        assertTrueAllTheTime(() -> {
            assertTrue(clusterB.getAMember().getMap(MAP_NAME).isEmpty());
            assertNoConnection(clusterA, wanReplication);
        }, 20);
    }

    @Test
    public void testGroupNameMismatch() {
        WanReplicationConfig wanRepConfig = clusterA.getConfig().getWanReplicationConfig(WAN_REPLICATION_SCHEME);
        WanBatchReplicationPublisherConfig wanPublisherConfig
                = wanRepConfig.getBatchPublisherConfigs().iterator().next();
        wanPublisherConfig.setGroupName(wanPublisherConfig.getGroupName() + "-wrong");

        clusterA.startCluster();
        clusterB.startCluster();

        fillMap(clusterA, MAP_NAME, 0, 10);
        assertTrueAllTheTime(() -> {
            assertTrue(clusterB.getAMember().getMap(MAP_NAME).isEmpty());
            assertNoConnection(clusterA, wanReplication);
        }, 20);
    }

    public void assertNoConnection(Cluster sourceCluster,
                                   WanReplication wanReplication) {
        final String setupName = wanReplication.getSetupName();
        final String targetClusterName = wanReplication.getTargetClusterName();
        for (HazelcastInstance instance : sourceCluster.getMembers()) {
            if (instance != null) {
                EnterpriseWanReplicationService service = wanReplicationService(instance);
                WanReplicationEndpoint endpoint = service.getEndpointOrNull(setupName, targetClusterName);
                if (endpoint != null) {
                    assertFalse(((WanBatchReplication) endpoint).isConnected());
                }
            }
        }
    }

    public void assertNegotiatedWanProtocolVersion(Cluster sourceCluster,
                                                   WanReplication wanReplication,
                                                   Version expectedVersion) {
        final String setupName = wanReplication.getSetupName();
        final String targetClusterName = wanReplication.getTargetClusterName();
        for (HazelcastInstance instance : sourceCluster.getMembers()) {
            if (instance != null) {
                EnterpriseWanReplicationService service = wanReplicationService(instance);
                WanReplicationEndpoint endpoint = service.getEndpointOrNull(setupName, targetClusterName);
                if (endpoint != null) {
                    WanConnectionManager manager = ((WanBatchReplication) endpoint).getConnectionManager();
                    for (WanConnectionWrapper wrapper : manager.getConnectionPool().values()) {
                        WanProtocolNegotiationResponse response = wrapper.getNegotiationResponse();
                        Version chosenWanProtocolVersion = response.getChosenWanProtocolVersion();
                        assertEquals(expectedVersion, chosenWanProtocolVersion);
                    }
                }
            }
        }
    }

    private void setSupportedWanProtocolVersions(Cluster cluster, Version... versions) {
        for (HazelcastInstance member : cluster.getMembers()) {
            when(getWanReplicationService(member).getSupportedWanProtocolVersions())
                    .thenReturn(Arrays.asList(versions));
        }
    }
}
