package com.hazelcast.wan.fw;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.DefaultTaskProgress;
import com.hazelcast.test.ProgressCheckerTask;
import com.hazelcast.test.TaskProgress;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertCompletesEventually;

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
        fillMap(aClusterInstance, mapName, start, end, valuePrefix, loadLatch);
    }

    public static void fillMap(HazelcastInstance aClusterInstance, String mapName, int start, int end) {
        fillMap(aClusterInstance, mapName, start, end, null, null);
    }

    public static void fillMap(HazelcastInstance aClusterInstance, String mapName, int start, int end, String valuePrefix,
                               CountDownLatch loadLatch) {
        IMap<Integer, String> m = aClusterInstance.getMap(mapName);
        for (int key = start; key < end; key++) {
            m.put(key, valuePrefix + key);
            if (loadLatch != null) {
                loadLatch.countDown();
            }
        }
    }

    public static void removeFromMap(Cluster cluster, String mapName, int start, int end) {
        HazelcastInstance aClusterInstance = cluster.getAMember();
        IMap<Integer, String> m = aClusterInstance.getMap(mapName);
        for (int key = start; key < end; key++) {
            m.remove(key);
        }
    }

    public static void verifyMapReplicated(Cluster sourceCluster, Cluster targetCluster, String mapName) {
        HazelcastInstance sourceClusterInstance = sourceCluster.getAMember();
        final IMap<Object, Object> sourceMap = sourceClusterInstance.getMap(mapName);

        HazelcastInstance targetClusterInstance = targetCluster.getAMember();
        final IMap<Object, Object> targetMap = targetClusterInstance.getMap(mapName);

        assertCompletesEventually(new ReplicationProgressCheckerTask(sourceMap, targetMap));
    }

    private static class ReplicationProgressCheckerTask implements ProgressCheckerTask {
        private final IMap<Object, Object> sourceMap;
        private final IMap<Object, Object> targetMap;

        private ReplicationProgressCheckerTask(IMap<Object, Object> sourceMap, IMap<Object, Object> targetMap) {
            this.sourceMap = sourceMap;
            this.targetMap = targetMap;
        }

        @Override
        public TaskProgress checkProgress() {
            int totalKeys = 0;
            int replicatedKeys = 0;
            for (Object key : sourceMap.keySet()) {
                totalKeys++;
                if (targetMap.containsKey(key)) {
                    replicatedKeys++;
                }
            }

            return new DefaultTaskProgress(totalKeys, replicatedKeys);
        }
    }
}
