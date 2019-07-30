package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.management.events.AddWanConfigIgnoredEvent;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.management.events.EventMetadata;
import com.hazelcast.internal.management.events.WanConfigurationAddedEvent;
import com.hazelcast.internal.management.events.WanConfigurationExtendedEvent;
import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.nio.NetworkingService;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;

import java.io.IOException;

import static com.hazelcast.enterprise.wan.impl.ConfigEventTest.WanConfigEventMatcher.addedEventMatcher;
import static com.hazelcast.enterprise.wan.impl.ConfigEventTest.WanConfigEventMatcher.extendedMatcher;
import static com.hazelcast.enterprise.wan.impl.ConfigEventTest.WanConfigEventMatcher.ignoredMatcher;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.Cluster.clusterC;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ConfigEventTest extends HazelcastTestSupport {

    private static final String EXISTING_WAN_CONFIG_NAME = "abc";
    private static final String MAP_NAME = "dummyMap";

    private Cluster clusterA;
    private Cluster clusterB;
    private Cluster clusterC;

    private TestHazelcastInstanceFactory factory = new WanServiceSpyingTestInstanceFactory();

    @Before
    public void initClusters() {
        clusterA = clusterA(factory, 2).setup();
        clusterB = clusterB(factory, 2).setup();
        clusterC = clusterC(factory, 2).setup();

        WanReplication wanReplication = replicate()
                .from(clusterA)
                .to(clusterB)
                .withSetupName(EXISTING_WAN_CONFIG_NAME)
                .setup();

        clusterA.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();
    }

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Test
    public void testAddWanConfigWithNewWanReplicationName() throws IOException {
        clusterA.startCluster();
        clusterB.startCluster();

        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName("new-name")
                .setup();

        HazelcastInstance coordinatorInstance = clusterA.getAMember();
        addWanReplicationConfig(coordinatorInstance, toBReplication);

        verify(getWanReplicationService(coordinatorInstance), times(1)).emitManagementCenterEvent(argThat(addedEventMatcher("new-name")));

        for (HazelcastInstance instance : clusterA.getMembers()) {
            verify(getWanReplicationService(instance), never()).emitManagementCenterEvent(isA(AddWanConfigIgnoredEvent.class));
            verify(getWanReplicationService(instance), never()).emitManagementCenterEvent(isA(WanConfigurationExtendedEvent.class));
        }
    }

    @Test
    public void testAddWanConfigWithExistingReplicationName() throws IOException {
        clusterA.startCluster();
        clusterB.startCluster();

        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(EXISTING_WAN_CONFIG_NAME)
                .setup();

        HazelcastInstance coordinatorInstance = clusterA.getAMember();
        addWanReplicationConfig(coordinatorInstance, toBReplication);

        verify(getWanReplicationService(coordinatorInstance), times(1)).emitManagementCenterEvent(argThat(ignoredMatcher(EXISTING_WAN_CONFIG_NAME)));

        for (HazelcastInstance instance : clusterA.getMembers()) {
            verify(getWanReplicationService(instance), never()).emitManagementCenterEvent(isA(WanConfigurationAddedEvent.class));
            verify(getWanReplicationService(instance), never()).emitManagementCenterEvent(isA(WanConfigurationExtendedEvent.class));
        }
    }

    @Test
    public void testAddWanConfigWithExistingReplicationName_newPublisher() throws IOException {
        clusterA.startCluster();
        clusterB.startCluster();

        WanReplication toCReplication = replicate()
                .to(clusterC)
                .withSetupName(EXISTING_WAN_CONFIG_NAME)
                .setup();

        HazelcastInstance coordinatorInstance = clusterA.getAMember();
        addWanReplicationConfig(coordinatorInstance, toCReplication);

        verify(getWanReplicationService(coordinatorInstance), times(1)).emitManagementCenterEvent(argThat(extendedMatcher(EXISTING_WAN_CONFIG_NAME, "C")));

        for (HazelcastInstance instance : clusterA.getMembers()) {
            verify(getWanReplicationService(instance), never()).emitManagementCenterEvent(isA(AddWanConfigIgnoredEvent.class));
        }
    }

    private AddWanConfigResult addWanReplicationConfig(HazelcastInstance instance, WanReplication wanReplication) {
        return getWanReplicationService(instance).addWanReplicationConfig(wanReplication.getConfig());
    }

    private EnterpriseWanReplicationService getWanReplicationService(HazelcastInstance instance) {
        return (EnterpriseWanReplicationService) getNodeEngineImpl(instance).getWanReplicationService();
    }

    class WanServiceSpyingTestInstanceFactory extends TestHazelcastInstanceFactory {
        @Override
        public HazelcastInstance newHazelcastInstance(Config config) {
            String instanceName = config != null ? config.getInstanceName() : null;
            if (TestEnvironment.isMockNetwork()) {
                config = initOrCreateConfig(config);
                NodeContext nodeContext = this.registry.createNodeContext(this.nextAddress(config.getNetworkConfig().getPort()));
                return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, new WanSpyingNodeContextDelegate(nodeContext));
            } else {
                return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, new WanSpyingNodeContextDelegate(new DefaultNodeContext()));
            }
        }
    }

    class WanSpyingNodeContextDelegate implements NodeContext {

        private NodeContext delegate;

        WanSpyingNodeContextDelegate(NodeContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new WanSpyingNodeExtension(node);
        }

        @Override
        public AddressPicker createAddressPicker(Node node) {
            return this.delegate.createAddressPicker(node);
        }

        @Override
        public Joiner createJoiner(Node node) {
            return this.delegate.createJoiner(node);
        }

        @Override
        public NetworkingService createNetworkingService(Node node, ServerSocketRegistry serverSocketRegistry) {
            return this.delegate.createNetworkingService(node, serverSocketRegistry);
        }
    }

    class WanSpyingNodeExtension extends EnterpriseNodeExtension {

        EnterpriseWanReplicationService wanReplicationService;

        WanSpyingNodeExtension(Node node) {
            super(node);
            wanReplicationService = spy(new EnterpriseWanReplicationService(node));
        }

        @Override
        public <T> T createService(Class<T> clazz) {
            return clazz.isAssignableFrom(WanReplicationService.class) ? (T) wanReplicationService : super.createService(clazz);
        }
    }

    static class WanConfigEventMatcher implements ArgumentMatcher<Event> {
        private final EventMetadata.EventType eventType;
        private final JsonObject verifierObject;

        static WanConfigEventMatcher addedEventMatcher(String wanName) {
            return new WanConfigEventMatcher(EventMetadata.EventType.WAN_CONFIGURATION_ADDED,
                    Json.object().add("wanConfigName", wanName));
        }

        static WanConfigEventMatcher extendedMatcher(String wanName, String... publisherIds) {
            return new WanConfigEventMatcher(EventMetadata.EventType.WAN_CONFIGURATION_EXTENDED,
                    Json.object()
                            .add("wanConfigName", wanName)
                            .add("wanPublisherIds", Json.array(publisherIds)));
        }

        static WanConfigEventMatcher ignoredMatcher(String wanName) {
            return new WanConfigEventMatcher(EventMetadata.EventType.ADD_WAN_CONFIGURATION_IGNORED,
                    Json.object().add("wanConfigName", wanName));
        }

        WanConfigEventMatcher(EventMetadata.EventType eventType, JsonObject verifierObject) {
            this.eventType = eventType;
            this.verifierObject = verifierObject;
        }

        @Override
        public boolean matches(Event event) {
            if (event.getType() != eventType) {
                return false;
            }
            JsonObject eventObject = event.toJson();
            if (!verifierObject.get("wanConfigName").asString().equals(eventObject.getString("wanConfigName", null))) {
                return false;
            }
            JsonValue eventPublisherIdsValue = eventObject.get("wanPublisherIds");
            JsonValue verifierPublisherIdsValue = verifierObject.get("wanPublisherIds");
            if (verifierPublisherIdsValue != null && eventPublisherIdsValue != null) {
                JsonArray publisherIds = verifierPublisherIdsValue.asArray();
                JsonArray eventPublisherIds = eventPublisherIdsValue.asArray();
                if (!publisherIds.equals(eventPublisherIds)) {
                    return false;
                }
            } else if (verifierPublisherIdsValue != null || eventPublisherIdsValue != null) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "WanConfigEventMatcher{"
                    + "eventType=" + eventType
                    + ", verifierObject=" + verifierObject
                    + '}';
        }
    }
}
