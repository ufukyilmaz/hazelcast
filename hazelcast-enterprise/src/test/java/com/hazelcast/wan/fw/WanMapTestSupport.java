package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertTrue;

public class WanMapTestSupport {
    private WanMapTestSupport() {
    }

    public static void fillMap(Cluster cluster, String mapName, int start, int end) {
        fillMap(cluster, mapName, start, end, cluster.getName(), null);
    }

    public static void fillMap(Cluster cluster, String mapName, int start, int end, String valuePrefix) {
        fillMap(cluster, mapName, start, end, valuePrefix, null);
    }

    public static void fillMap(Cluster cluster, String mapName, int start, int end, CountDownLatch loadLatch) {
        fillMap(cluster, mapName, start, end, cluster.getName(), loadLatch);
    }

    public static void fillMap(Cluster cluster, String mapName, int start, int end, String valuePrefix,
                               CountDownLatch loadLatch) {
        HazelcastInstance aClusterInstance = cluster.getAMember();
        IMap<Integer, String> m = aClusterInstance.getMap(mapName);
        for (; start < end; start++) {
            m.put(start, valuePrefix + start);
            if (loadLatch != null) {
                loadLatch.countDown();
            }
        }
    }

    public static void verifyMapReplicated(Cluster sourceCluster, Cluster targetCluster, String mapName) {
        HazelcastInstance sourceClusterInstance = sourceCluster.getAMember();
        final IMap<Object, Object> sourceMap = sourceClusterInstance.getMap(mapName);

        HazelcastInstance targetClusterInstance = targetCluster.getAMember();
        final IMap<Object, Object> targetMap = targetClusterInstance.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(verifyMapReplicatedInternal(sourceMap, targetMap));
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private static boolean verifyMapReplicatedInternal(IMap<Object, Object> sourceMap, IMap<Object, Object> targetMap) {
        for (Object key : sourceMap.keySet()) {
            if (!targetMap.containsKey(key)) {
                return false;
            }
        }

        return true;
    }
}
