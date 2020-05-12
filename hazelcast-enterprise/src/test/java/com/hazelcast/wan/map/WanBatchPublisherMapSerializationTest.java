package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.internal.serialization.impl.portable.PortableHook;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanBatchPublisherMapSerializationTest extends WanMapTestSupport {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        config.getMapConfig("default")
                .setInMemoryFormat(getMemoryFormat());

        return config;
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Test
    public void noFailuresWhenTargetClusterDoesNotContainClass() throws Exception {
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

        aMap.put(o, o);
        assertSizeEventually(1, bMap);

        aMap.delete(o);
        assertSizeEventually(0, bMap);


        // assert no publisher in the entire cluster encountered a failure
        assertPublisherFailureCountEventually(clusterA, 0, publisherName);
    }

    private static void assertPublisherFailureCountEventually(final HazelcastInstance[] cluster,
                                                              final int expectedFailureCount,
                                                              final String publisherName) {
        assertTrueEventually(() -> assertPublisherFailureCount(cluster, expectedFailureCount, publisherName));
    }

    private static void assertPublisherFailureCount(HazelcastInstance[] cluster, int expectedFailureCount, String publisherName) {
        for (HazelcastInstance instance : cluster) {
            final NodeEngineImpl nodeEngine = Accessors.getNode(instance).nodeEngine;
            final EnterpriseWanReplicationService s = nodeEngine.getService(EnterpriseWanReplicationService.SERVICE_NAME);
            final DelegatingWanScheme atob = s.getWanReplicationPublishers(publisherName);
            int failureCount = 0;
            for (WanPublisher publisher : atob.getPublishers()) {
                final WanBatchPublisher batchReplication = (WanBatchPublisher) publisher;
                failureCount += batchReplication.getFailedTransmissionCount();
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
}
