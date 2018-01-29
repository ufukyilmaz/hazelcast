package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.wan.map.filter.DummyMapWanFilter;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class MapWanBatchReplicationTest extends AbstractMapWanReplicationTest {

    @After
    @Override
    public void cleanup() {
        super.cleanup();
        unsetUseDeleteWhenProcessingRemoveEvents();
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Test
    public void failWhenTargetClusterDoesNotContainClassAndUsingRemove() throws Exception {
        // false by default
        unsetUseDeleteWhenProcessingRemoveEvents();
        testClassNotFoundExceptionTargetCluster(1);
    }

    @Test
    public void noFailuresWhenTargetClusterDoesNotContainClassAndUsingDelete() throws Exception {
        setUseDeleteWhenProcessingRemoveEvents(true);
        testClassNotFoundExceptionTargetCluster(0);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void failStartupWhenEndpointsAreMisconfigured() {
        final String publisherSetup = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, publisherSetup, PassThroughMergePolicy.class.getName());

        for (WanPublisherConfig publisherConfig : configA.getWanReplicationConfig(publisherSetup).getWanPublisherConfigs()) {
            final Map<String, Comparable> properties = publisherConfig.getProperties();
            final String endpoints = (String) properties.get(ENDPOINTS.key());
            final String endpointsWithError = endpoints.replaceFirst("\\.", "\\.mumboJumbo\n");
            properties.put(ENDPOINTS.key(), endpointsWithError);
        }

        startClusterA();
        createDataIn(clusterA, "map", 1, 10);
    }

    @Test
    @Ignore
    public void recoverAfterTargetClusterFailure() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();

        createDataIn(clusterA, "map", 0, 1000);

        sleepSeconds(10);

        clusterA[0].shutdown();
        sleepSeconds(10);
        startClusterB();
        assertDataInFrom(clusterB, "map", 0, 1000, getNode(clusterA[1]).getConfig().getGroupConfig().getName());
    }

    @Test
    public void testMapWanFilter() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), DummyMapWanFilter.class.getName());
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 1, 10);
        assertKeysIn(clusterB, "map", 1, 2);
        assertKeysNotIn(clusterB, "map", 2, 10);
    }

    @Test
    public void testMigration() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());

        initCluster(singleNodeA, configA);
        createDataIn(singleNodeA, "map", 0, 1000);
        initCluster(singleNodeC, configA);

        initCluster(clusterB, configB);

        assertDataInFrom(clusterB, "map", 0, 1000, singleNodeC[0].getConfig().getGroupConfig().getName());
    }

    private void testClassNotFoundExceptionTargetCluster(int expectedFailureCount) throws InstantiationException, IllegalAccessException {
        clusterA = new HazelcastInstance[1];
        clusterB = new HazelcastInstance[1];
        final String publisherName = "atob";
        final String mapName = "map";

        setupReplicateFrom(configA, configB, clusterB.length, publisherName, PassThroughMergePolicy.class.getName());

        // create class in a separate classloader
        final URLClassLoader childClassloader = new URLClassLoader(new URL[]{}, PortableHook.class.getClassLoader());
        final Class<?> c = newSerializableClass("TestClass", childClassloader);
        final Object o = c.newInstance();

        configA.setClassLoader(childClassloader)
                .getMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        configB.getMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);

        startClusterA();
        startClusterB();

        final IMap<Object, Object> aMap = getNode(clusterA).getMap(mapName);
        final IMap<Object, Object> bMap = getNode(clusterB).getMap(mapName);

        aMap.put("a", o);
        assertSizeEventually(1, bMap);

        aMap.delete("a");
        assertSizeEventually(0, bMap);


        // assert no publisher in the entire cluster encountered a failure
        for (HazelcastInstance instance : clusterA) {
            final NodeEngineImpl nodeEngine = getNode(instance).nodeEngine;
            final EnterpriseWanReplicationService s = nodeEngine.getService(EnterpriseWanReplicationService.SERVICE_NAME);
            final WanReplicationPublisherDelegate atob = (WanReplicationPublisherDelegate) s.getWanReplicationPublisher(publisherName);
            int failureCount = 0;
            for (WanReplicationEndpoint endpoint : atob.getEndpoints()) {
                final WanBatchReplication batchReplication = (WanBatchReplication) endpoint;
                failureCount += batchReplication.getFailedTransmitCount();
            }
            assertEquals("Encountered failure in WAN replication", expectedFailureCount, failureCount);
        }
    }

    private Class<?> newSerializableClass(String classname, ClassLoader classLoader) {
        DynamicType.Unloaded<?> def = new ByteBuddy()
                .subclass(Serializable.class)
                .name(classname)
                .make();
        return def.load(classLoader).getLoaded();
    }

    private static void setUseDeleteWhenProcessingRemoveEvents(boolean value) {
        System.setProperty("hazelcast.wan.map.useDeleteWhenProcessingRemoveEvents", String.valueOf(value));
    }

    private static void unsetUseDeleteWhenProcessingRemoveEvents() {
        System.clearProperty("hazelcast.wan.map.useDeleteWhenProcessingRemoveEvents");
    }
}
