package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.enterprise.wan.WANReplicationQueueFullException;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public abstract class AbstractMapWanReplicationTest extends MapWanReplicationTestSupport {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setInMemoryFormat(getMemoryFormat());
        return config;
    }

    // V topo config 1 passive replicar, 2 producers
    @Test
    public void VTopo_1passiveReplicar_2producers_Test_PassThroughMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterB, "map", 1000, 2000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterC, "map", 1000, 2000, clusterB);

        createDataIn(clusterB, "map", 0, 1);
        assertDataInFrom(clusterC, "map", 0, 1, clusterB);

        removeDataIn(clusterA, "map", 0, 500);
        removeDataIn(clusterB, "map", 1500, 2000);

        assertKeysNotIn(clusterC, "map", 0, 500);
        assertKeysNotIn(clusterC, "map", 1500, 2000);

        assertKeysIn(clusterC, "map", 500, 1500);

        removeDataIn(clusterA, "map", 500, 1000);
        removeDataIn(clusterB, "map", 1000, 1500);

        assertKeysNotIn(clusterC, "map", 0, 2000);
        assertDataSizeEventually(clusterC, "map", 0);
    }

    @Test
    public void Vtopo_TTL_Replication_Issue254() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());

        configA.getMapConfig("default").setTimeToLiveSeconds(20);
        configB.getMapConfig("default").setTimeToLiveSeconds(20);
        configC.getMapConfig("default").setTimeToLiveSeconds(20);

        startAllClusters();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFromNoSleep(clusterC, "map", 0, 10, clusterA);

        createDataIn(clusterB, "map", 10, 20);
        assertDataInFromNoSleep(clusterC, "map", 10, 20, clusterB);

        sleepSeconds(20);
        assertKeysNotIn(clusterA, "map", 0, 10);
        assertKeysNotIn(clusterB, "map", 10, 20);
        assertKeysNotIn(clusterC, "map", 0, 20);
    }

    //"Issue #1371 this topology requested hear https://groups.google.com/forum/#!msg/hazelcast/73jJo9W_v4A/5obqKMDQAnoJ")
    @Test
    public void VTopo_1activeActiveReplicar_2producers_Test_PassThroughMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());

        setupReplicateFrom(configC, configA, clusterA.length, "ctoab", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configC, configB, clusterB.length, "ctoab", PassThroughMergePolicy.class.getName());

        startAllClusters();

        printAllReplicarConfig();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterB, "map", 1000, 2000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);
        assertDataInFrom(clusterC, "map", 1000, 2000, clusterB);

        assertDataInFrom(clusterA, "map", 1000, 2000, clusterB);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);
    }

    @Test
    public void VTopo_1passiveReplicar_2producers_Test_PutIfAbsentMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PutIfAbsentMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PutIfAbsentMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 100);
        createDataIn(clusterB, "map", 100, 200);

        assertDataInFrom(clusterC, "map", 0, 100, clusterA);
        assertDataInFrom(clusterC, "map", 100, 200, clusterB);

        createDataIn(clusterB, "map", 0, 100);
        assertDataInFrom(clusterC, "map", 0, 100, clusterA);

        assertDataSizeEventually(clusterC, "map", 200);
    }


    @Test
    public void VTopo_1passiveReplicar_2producers_Test_LatestUpdateMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", LatestUpdateMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", LatestUpdateMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterB);

        assertDataSizeEventually(clusterC, "map", 1000);

        removeDataIn(clusterA, "map", 0, 500);
        assertKeysNotIn(clusterC, "map", 0, 500);

        removeDataIn(clusterB, "map", 500, 1000);
        assertKeysNotIn(clusterC, "map", 500, 1000);

        assertDataSizeEventually(clusterC, "map", 0);
    }

    @Test
    public void VTopo_1passiveReplicar_2producers_Test_HigherHitsMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitsMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 1000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterA);

        increaseHitCount(clusterB, "map", 0, 1000, 100);
        createDataIn(clusterB, "map", 0, 1000);

        assertDataInFrom(clusterC, "map", 0, 1000, clusterB);
    }

    //("Issue #1368 multi replicar topology cluster A replicates to B and C")
    @Test
    public void VTopo_2passiveReplicar_1producer_Test() {
        String replicaName = "multiReplica";
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configC, clusterC.length, replicaName, PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterC, "map", 0, 1000);

        removeDataIn(clusterA, "map", 0, 1000);

        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterC, "map", 0, 1000);

        assertDataSizeEventually(clusterB, "map", 0);
        assertDataSizeEventually(clusterC, "map", 0);
    }

    @Test
    public void linkTopo_ActiveActiveReplication_Test() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 1000, 2000);
        assertDataInFrom(clusterA, "map", 1000, 2000, clusterB);

        removeDataIn(clusterA, "map", 1500, 2000);
        assertKeysNotIn(clusterB, "map", 1500, 2000);

        removeDataIn(clusterB, "map", 0, 500);
        assertKeysNotIn(clusterA, "map", 0, 500);

        assertKeysIn(clusterA, "map", 500, 1500);
        assertKeysIn(clusterB, "map", 500, 1500);

        assertDataSizeEventually(clusterA, "map", 1000);
        assertDataSizeEventually(clusterB, "map", 1000);
    }


    @Test
    public void linkTopo_ActiveActiveReplication_2clusters_Test_HigherHitsMapMergePolicy() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", HigherHitsMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFrom(clusterB, "map", 0, 1000, clusterA);

        increaseHitCount(clusterB, "map", 0, 500, 1000);
        createDataIn(clusterB, "map", 0, 500);
        assertDataInFrom(clusterA, "map", 0, 500, clusterB);
        sleepSeconds(10);
    }

    @Test
    public void chainTopo_2passiveReplicars_1producer() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysIn(clusterB, "map", 0, 1000);
        assertDataSizeEventually(clusterB, "map", 1000);

        assertKeysIn(clusterC, "map", 0, 1000);
        assertDataSizeEventually(clusterC, "map", 1000);
    }

    @Test
    public void wan_events_should_be_processed_in_order() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 10);
        removeAndCreateDataIn(clusterA, "map", 0, 10);

        assertKeysIn(clusterB, "map", 0, 10);
        assertDataSizeEventually(clusterB, "map", 10);
        sleepSeconds(10);
    }

    @Test
    public void replicationRing() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configC, configA, clusterA.length, "ctoa", PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysIn(clusterB, "map", 0, 1000);
        assertDataSizeEventually(clusterB, "map", 1000);

        assertKeysIn(clusterC, "map", 0, 1000);
        assertDataSizeEventually(clusterC, "map", 1000);
    }

    @Test
    public void linkTopo_ActiveActiveReplication_Threading_Test() throws InterruptedException, BrokenBarrierException {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        CyclicBarrier gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            public void go() {
                createDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            public void go() {
                createDataIn(clusterB, "map", 500, 1500);
            }
        });
        gate.await();

        assertDataInFrom(clusterB, "map", 0, 500, clusterA);
        assertDataInFrom(clusterA, "map", 1000, 1500, clusterB);
        assertKeysIn(clusterA, "map", 500, 1000);

        gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            public void go() {
                removeDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            public void go() {
                removeDataIn(clusterB, "map", 500, 1500);
            }
        });
        gate.await();

        assertKeysNotIn(clusterA, "map", 0, 1500);
        assertKeysNotIn(clusterB, "map", 0, 1500);

        assertDataSizeEventually(clusterA, "map", 0);
        assertDataSizeEventually(clusterB, "map", 0);
    }

    @Test
    public void checkAuthorization() {
        String groupName = configB.getGroupConfig().getName();
        configB.getGroupConfig().setName("wrongGroupName");
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        configB.getGroupConfig().setName(groupName);
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 0, 10);
        sleepSeconds(10);
        assertKeysNotIn(clusterB, "map", 0, 10);
    }

    @Test
    public void checkErasingMapMergePolicy() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", DeleteMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterB, "map", 0, 100);
        createDataIn(clusterA, "map", 0, 100);
        assertKeysNotIn(clusterB, "map", 0, 100);
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrun() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanTargetClusterConfig targetClusterConfig = wanConfig.getTargetClusterConfigs().get(0);
        targetClusterConfig.setQueueCapacity(10);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION);
        startClusterA();
        createDataIn(clusterA, "map", 0, 1000);
    }
}
