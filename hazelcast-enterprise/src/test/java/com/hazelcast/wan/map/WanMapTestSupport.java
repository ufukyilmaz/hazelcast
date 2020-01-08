package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.impl.replication.AbstractWanPublisher;
import com.hazelcast.map.IMap;
import com.hazelcast.wan.WanTestSupport;
import org.junit.Before;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public abstract class WanMapTestSupport extends WanTestSupport {

    private static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE = 3 * 60;

    @Before
    public void setup() {
        configA = getConfig();
        configA.setClusterName("A");
        configA.setInstanceName("confA-" + UUID.randomUUID() + "-");
        configA.getNetworkConfig().setPort(5701);

        configB = getConfig();
        configB.setClusterName("B");
        configB.setInstanceName("confB-" + UUID.randomUUID() + "-");
        configB.getNetworkConfig().setPort(5801);

        configC = getConfig();
        configC.setClusterName("C");
        configC.setInstanceName("confC-" + UUID.randomUUID() + "-");
        configC.getNetworkConfig().setPort(5901);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy,
                                      String filter) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, filter, ConsistencyCheckStrategy.NONE);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy,
                                      ConsistencyCheckStrategy consistencyCheckStrategy) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, null, consistencyCheckStrategy);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, null, ConsistencyCheckStrategy.NONE);
    }

    protected void setupReplicateFrom(Config fromConfig,
                                      String publisherId,
                                      Class<?> publisherClass,
                                      String setupName,
                                      String policy,
                                      String filter) {
        WanReplicationConfig wanReplicationConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanReplicationConfig == null) {
            wanReplicationConfig = new WanReplicationConfig();
            wanReplicationConfig.setName(setupName);
        }

        if (AbstractWanPublisher.class.isAssignableFrom(publisherClass)) {
            wanReplicationConfig.addBatchReplicationPublisherConfig(new WanBatchPublisherConfig()
                    .setPublisherId(publisherId)
                    .setClassName(publisherClass.getName()));
        } else {
            wanReplicationConfig.addCustomPublisherConfig(new WanCustomPublisherConfig()
                    .setPublisherId(publisherId)
                    .setClassName(publisherClass.getName()));
        }


        WanReplicationRef wanRef = new WanReplicationRef()
                .setName(setupName)
                .setMergePolicyClassName(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }

        fromConfig.addWanReplicationConfig(wanReplicationConfig);
        fromConfig.getMapConfig("default").setWanReplicationRef(wanRef);
    }

    protected void setupReplicateFrom(Config fromConfig,
                                      Config toConfig,
                                      int clusterSz,
                                      String setupName,
                                      String policy,
                                      String filter,
                                      ConsistencyCheckStrategy consistencyCheckStrategy) {
        WanReplicationConfig wanReplicationConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanReplicationConfig == null) {
            wanReplicationConfig = new WanReplicationConfig();
            wanReplicationConfig.setName(setupName);
        }

        wanReplicationConfig.addBatchReplicationPublisherConfig(targetCluster(toConfig, clusterSz, consistencyCheckStrategy));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicyClassName(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }

        fromConfig.addWanReplicationConfig(wanReplicationConfig);
        fromConfig.getMapConfig("default").setWanReplicationRef(wanRef);
    }

    // should be protected, used by hazelcast-solace
    public static void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        createDataIn(cluster, mapName, start, end, (CountDownLatch) null);
    }

    public static void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end, CountDownLatch latch) {
        HazelcastInstance node = getNode(cluster);
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.put(start, node.getConfig().getClusterName() + start);
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    public static void createDataIn(HazelcastInstance[] cluster, String mapName, int start, int end, String value) {
        HazelcastInstance node = getNode(cluster);
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.put(start, value);
        }
    }

    public static void increaseHitCount(HazelcastInstance[] cluster, String mapName, int start, int end, int repeat) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            for (int i = 0; i < repeat; i++) {
                m.get(start);
            }
        }
    }

    public static void removeDataIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            m.remove(start);
        }
    }

    public static void assertKeysIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            assertContainsKey(m, start);
        }
    }

    public static <T> void assertContainsKey(IMap<T, ?> map, T key) {
        assertTrue("Map '" + map + "' does not contain key '" + key + "' ", map.containsKey(key));
    }

    public static void assertDataInFrom(HazelcastInstance[] targetCluster, String mapName, int start, int end, HazelcastInstance[] sourceCluster) {
        String sourceClusterName = getNode(sourceCluster).getConfig().getClusterName();
        assertDataInFrom(targetCluster, mapName, start, end, sourceClusterName);
    }

    public static void assertDataInFrom(HazelcastInstance[] targetCluster, String mapName, int start, int end, String sourceClusterName) {
        HazelcastInstance node = getNode(targetCluster);

        IMap m = node.getMap(mapName);
        for (; start < end; start++) {
            Object v = m.get(start);
            assertEquals(sourceClusterName + start, v);
        }
    }

    public static boolean checkKeysNotIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            if (m.containsKey(start)) {
                return false;
            }
        }
        return true;
    }

    public static void assertDataSizeEventually(final HazelcastInstance[] cluster, final String mapName, final int size) {
        assertTrueEventually(() -> {
            IMap m = getMap(cluster, mapName);
            assertEquals(size, m.size());
        });
    }

    public static void assertKeysInEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end) {
        assertTrueEventually(() -> assertKeysIn(cluster, mapName, start, end), ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    public static void assertDataInFromEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end, final String sourceClusterName) {
        assertTrueEventually(() -> assertDataInFrom(cluster, mapName, start, end, sourceClusterName), ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    public static void assertDataInFromEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end, final HazelcastInstance[] sourceCluster) {
        assertTrueEventually(() -> assertDataInFrom(cluster, mapName, start, end, sourceCluster), ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    public static void assertKeysNotInEventually(final HazelcastInstance[] cluster, final String mapName, final int start, final int end) {
        assertTrueEventually(() -> assertTrue(checkKeysNotIn(cluster, mapName, start, end)), ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    public static void removeAndCreateDataIn(HazelcastInstance[] cluster, String mapName, int start, int end) {
        HazelcastInstance node = getNode(cluster);
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.remove(start);
            m.put(start, node.getConfig().getClusterName() + start);
        }
    }

    public static <K, V> IMap<K, V> getMap(HazelcastInstance[] cluster, String mapName) {
        HazelcastInstance node = getNode(cluster);
        return node.getMap(mapName);
    }
}
