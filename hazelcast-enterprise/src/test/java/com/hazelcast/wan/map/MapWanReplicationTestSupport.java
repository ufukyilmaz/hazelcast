package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.wan.WanReplicationTestSupport;
import org.junit.Before;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public abstract class MapWanReplicationTestSupport extends WanReplicationTestSupport {

    private int ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE = 3 * 60;

    @Before
    public void setup() {
        configA = getConfig();
        configA.getGroupConfig().setName("A");
        configA.setInstanceName("confA-" + UUID.randomUUID() + "-");
        configA.getNetworkConfig().setPort(5701);

        configB = getConfig();
        configB.getGroupConfig().setName("B");
        configB.setInstanceName("confB-" + UUID.randomUUID() + "-");
        configB.getNetworkConfig().setPort(5801);

        configC = getConfig();
        configC.getGroupConfig().setName("C");
        configC.setInstanceName("confC-" + UUID.randomUUID() + "-");
        configC.getNetworkConfig().setPort(5901);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, null);
    }

    void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy, String filter) {
        WanReplicationConfig wanConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
        }
        wanConfig.addWanPublisherConfig(targetCluster(toConfig, clusterSz));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicy(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }

        fromConfig.addWanReplicationConfig(wanConfig);
        fromConfig.getMapConfig("default").setWanReplicationRef(wanRef);
    }

    // should be protected, used by hazelcast-solace
    protected void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        createDataIn(cluster, mapName, start, end, (CountDownLatch) null);
    }

    protected void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end, CountDownLatch latch) {
        HazelcastInstance node = getNode(cluster);
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.put(start, node.getConfig().getGroupConfig().getName() + start);
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    protected void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end, String value) {
        HazelcastInstance node = getNode(cluster);
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.put(start, value);
        }
    }

    void increaseHitCount(HazelcastInstance[] cluster, String mapName, int start, int end, int repeat) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            for (int i = 0; i < repeat; i++) {
                m.get(start);
            }
        }
    }

    void removeDataIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            m.remove(start);
        }
    }

    void assertKeysIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            assertContainsKey(m, start);
        }
    }

    <T> void assertContainsKey(IMap<T, ?> map, T key) {
        assertTrue("Map '" + map + "' does not contain key '" + key + "' ", map.containsKey(key));
    }

    void assertDataInFrom(HazelcastInstance[] targetCluster, String mapName, int start, int end, HazelcastInstance[] sourceCluster) {
        String sourceGroupName = getNode(sourceCluster).getConfig().getGroupConfig().getName();
        assertDataInFrom(targetCluster, mapName, start, end, sourceGroupName);
    }

    void assertDataInFrom(HazelcastInstance[] targetCluster, String mapName, int start, int end, String sourceGroupName) {
        HazelcastInstance node = getNode(targetCluster);

        IMap m = node.getMap(mapName);
        for (; start < end; start++) {
            Object v = m.get(start);
            assertEquals(sourceGroupName + start, v);
        }
    }

    boolean checkKeysNotIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            if (m.containsKey(start)) {
                return false;
            }
        }
        return true;
    }

    void assertDataSizeEventually(final HazelcastInstance[] cluster, final String mapName, final int size) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                IMap m = getMap(cluster, mapName);
                assertEquals(size, m.size());
            }
        });
    }

    void assertKeysInEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertKeysIn(cluster, mapName, start, end);
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    protected void assertDataInFromEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end, final String sourceGroupName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertDataInFrom(cluster, mapName, start, end, sourceGroupName);
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    protected void assertDataInFromEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end, final HazelcastInstance[] sourceCluster) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertDataInFrom(cluster, mapName, start, end, sourceCluster);
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    void assertKeysNotInEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(checkKeysNotIn(cluster, mapName, start, end));
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    void assertKeysNotInAllTheTime(final HazelcastInstance[] cluster, final String mapName, final int start, final int end) {
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertTrue(checkKeysNotIn(cluster, mapName, start, end));
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    void removeAndCreateDataIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        HazelcastInstance node = getNode(cluster);
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.remove(start);
            m.put(start, node.getConfig().getGroupConfig().getName() + start);
        }
    }

    <K, V> IMap<K, V> getMap(HazelcastInstance[] cluster, String mapName) {
        HazelcastInstance node = getNode(cluster);
        return node.getMap(mapName);
    }
}
