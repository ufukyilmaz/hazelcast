package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.cluster.ClusterShutdownTest.assertNodesShutDownEventually;
import static com.hazelcast.cluster.ClusterShutdownTest.getNodes;
import static com.hazelcast.test.Accessors.getAddresses;
import static com.hazelcast.test.Accessors.getClusterService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HotRestartDistributedObjectsTest extends AbstractHotRestartClusterStartTest {

    @Test
    public void test_getDistributedObjects_afterHotRestartProcessCompletes() {
        HazelcastInstance[] instances = startNewInstances(4);
        for (String mapName : mapNames) {
            instances[0].getMap(mapName).put("foo", new Item());
        }
        for (String cacheName : cacheNames) {
            instances[0].getCacheManager().getCache(cacheName).put("baz", new Item());
        }

        int expected = mapNames.length + cacheNames.length;
        assertDistributedObjectsSize(expected, instances);

        Address[] addresses = getAddresses(instances);
        Node[] nodes = getNodes(instances);
        instances[0].getCluster().shutdown();
        assertNodesShutDownEventually(nodes);

        instances = restartInstances(addresses);
        assertDistributedObjectsSize(expected, instances);

        instances[0].getMap("onemore").put("foo", new Item());
        expected++;
        assertDistributedObjectsSize(expected, instances);

        HazelcastInstance newInstance = startNewInstance();
        assertTrue(getClusterService(newInstance).isJoined());
        assertDistributedObjectsSize(expected, newInstance);

        instances[0].getMap(mapNames[0]).destroy();
        expected--;
        assertDistributedObjectsSize(expected, instances);
        assertDistributedObjectsSize(expected, newInstance);
    }

    private static void assertDistributedObjectsSize(final int expected, final HazelcastInstance... instances) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                Collection<DistributedObject> objects = instance.getDistributedObjects();
                assertEquals(expected, objects.size());
            }
        });
    }

    @Override
    Config newConfig(ClusterHotRestartEventListener listener, HotRestartClusterDataRecoveryPolicy clusterStartPolicy) {
        Config config = super.newConfig(listener, clusterStartPolicy);
        for (MapConfig mapConfig : config.getMapConfigs().values()) {
            mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "attribute"));
        }
        return config;
    }

    public static class Item implements Serializable {
        private int attribute;
    }
}
