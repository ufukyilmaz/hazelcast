package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.impl.connection.WanConnectionWrapper;
import com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationResponse;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.wan.WanEnterpriseMapUpdateEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.wan.WanTestSupport.getWanReplicationService;
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
public class WanProtocolVersionSelectionTest extends HazelcastTestSupport {

    private static final String WAN_REPLICATION_SCHEME = "abc";
    private static final String MAP_NAME = "dummyMap";
    private static final int CUSTOM_FACTORY_ID = 100;
    private static final int CUSTOM_WAN_EVENT = 1;
    private static final Version V1_0 = Version.of(1, 0);
    private static final Version V1_1 = Version.of(1, 1);
    private static final Version V1_2 = Version.of(1, 2);

    private TestHazelcastInstanceFactory factory;
    private Cluster clusterA;
    private Cluster clusterB;
    private WanReplication wanReplication;

    @Before
    public void initClusters() {
        factory = new CustomNodeExtensionTestInstanceFactory(
                node -> new WanServiceMockingEnterpriseNodeExtension(node, spy(new EnterpriseWanReplicationService(node))));
        clusterA = clusterA(factory, 1, this::getConfig).setup();
        clusterB = clusterB(factory, 1, this::getConfig).setup();

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

    @Override
    protected Config getConfig() {
        Config c = smallInstanceConfig();
        c.getSerializationConfig()
         .addDataSerializableFactory(CUSTOM_FACTORY_ID, new CustomSerializerFactory());
        return c;
    }

    @Test
    public void testLatestWanProtocolIsSelected() {
        clusterA.startCluster();
        clusterB.startCluster();

        setSupportedWanProtocolVersions(clusterA, V1_2, V1_1, V1_0);
        setSupportedWanProtocolVersions(clusterB, V1_2, V1_1);

        fillMap(clusterA, MAP_NAME, 0, 10);
        verifyMapReplicated(clusterA, clusterB, MAP_NAME);
        assertNegotiatedWanProtocolVersion(clusterA, wanReplication, V1_2);
    }

    @Test
    public void testNoMatchingProtocol() {
        clusterA.startCluster();
        clusterB.startCluster();

        setSupportedWanProtocolVersions(clusterA, V1_1, V1_0);
        setSupportedWanProtocolVersions(clusterB, V1_2);

        fillMap(clusterA, MAP_NAME, 0, 10);
        assertTrueAllTheTime(() -> {
            assertTrue(clusterB.getAMember().getMap(MAP_NAME).isEmpty());
            assertNoConnection(clusterA, wanReplication);
        }, 20);
    }

    @Test
    public void testClusterNameMismatch() {
        WanReplicationConfig wanRepConfig = clusterA.getConfig().getWanReplicationConfig(WAN_REPLICATION_SCHEME);
        WanBatchPublisherConfig wanPublisherConfig
                = wanRepConfig.getBatchPublisherConfigs().iterator().next();
        wanPublisherConfig.setClusterName(wanPublisherConfig.getClusterName() + "-wrong");

        clusterA.startCluster();
        clusterB.startCluster();

        fillMap(clusterA, MAP_NAME, 0, 10);
        assertTrueAllTheTime(() -> {
            assertTrue(clusterB.getAMember().getMap(MAP_NAME).isEmpty());
            assertNoConnection(clusterA, wanReplication);
        }, 20);
    }

    @Test
    public void testSerializationVersion() {
        clusterA.startCluster();
        clusterB.startCluster();

        setSupportedWanProtocolVersions(clusterA, V1_2, V1_1, V1_0);
        setSupportedWanProtocolVersions(clusterB, V1_1, V1_0);

        String key = "key";
        String value = "value";

        publishCustomMapEvent(key, value, V1_1);
        assertTrueEventually(() -> assertEquals(value, clusterB.getAMember().getMap(MAP_NAME).get(key)), 30);
    }

    private void publishCustomMapEvent(Object key, Object value,
                                       Version expectedSerializationVersion) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(clusterA.getAMember());
        WanReplicationService service = nodeEngine.getWanReplicationService();
        DelegatingWanScheme publisher
                = service.getWanReplicationPublishers(wanReplication.getSetupName());
        SerializationService ss = nodeEngine.getSerializationService();

        WanEnterpriseMapCustomEvent event = new WanEnterpriseMapCustomEvent(
                MAP_NAME, ss.toData(key), ss.toData(value), expectedSerializationVersion);
        publisher.publishReplicationEvent(event);
    }

    public void assertNoConnection(Cluster sourceCluster,
                                   WanReplication wanReplication) {
        final String setupName = wanReplication.getSetupName();
        final String targetClusterName = wanReplication.getTargetClusterName();
        for (HazelcastInstance instance : sourceCluster.getMembers()) {
            if (instance != null) {
                EnterpriseWanReplicationService service = wanReplicationService(instance);
                WanPublisher publisher = service.getPublisherOrNull(setupName, targetClusterName);
                if (publisher != null) {
                    assertFalse(((WanBatchPublisher) publisher).isConnected());
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
                WanPublisher publisher = service.getPublisherOrNull(setupName, targetClusterName);
                if (publisher != null) {
                    WanConnectionManager manager = ((WanBatchPublisher) publisher).getConnectionManager();
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

    static class CustomSerializerFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == CUSTOM_WAN_EVENT) {
                return new WanEnterpriseMapCustomEvent();
            }
            return null;
        }
    }

    static class WanEnterpriseMapCustomEvent extends WanEnterpriseMapUpdateEvent {

        private Version expectedSerializationVersion;

        WanEnterpriseMapCustomEvent() {
        }

        WanEnterpriseMapCustomEvent(String mapName,
                                    Data key,
                                    Data value,
                                    Version expectedSerializationVersion) {
            super(mapName, new PassThroughMergePolicy<>(), new SimpleEntryView<>(key, value), 0);
            this.expectedSerializationVersion = expectedSerializationVersion;
        }

        @Override
        public int getFactoryId() {
            return CUSTOM_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CUSTOM_WAN_EVENT;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            BufferObjectDataOutput dataOutput = (BufferObjectDataOutput) out;
            out.writeObject(expectedSerializationVersion);
            assertEquals(expectedSerializationVersion, dataOutput.getWanProtocolVersion());
            super.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            BufferObjectDataInput dataInput = (BufferObjectDataInput) in;
            expectedSerializationVersion = in.readObject();
            assertEquals(expectedSerializationVersion, dataInput.getWanProtocolVersion());
            super.readData(in);
        }
    }
}
