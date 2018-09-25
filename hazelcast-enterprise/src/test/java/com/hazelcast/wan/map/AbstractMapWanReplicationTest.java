package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.internal.jmx.MBeanDataHolder;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.UninitializableWanEndpoint;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.custom.CustomWanConsumer;
import com.hazelcast.wan.map.filter.NoFilterMapWanFilter;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.TASK_PERIOD_SECONDS;
import static org.junit.Assert.assertEquals;

public abstract class AbstractMapWanReplicationTest extends MapWanReplicationTestSupport {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        config.getMapConfig("default")
              .setInMemoryFormat(getMemoryFormat());

        return config;
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withPassThroughMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        createDataIn(clusterB, "map", 1000, 2000);

        assertDataInFromEventually(clusterC, "map", 0, 1000, clusterA);
        assertDataInFromEventually(clusterC, "map", 1000, 2000, clusterB);

        createDataIn(clusterB, "map", 0, 1);
        assertDataInFromEventually(clusterC, "map", 0, 1, clusterB);

        removeDataIn(clusterA, "map", 0, 500);
        removeDataIn(clusterB, "map", 1500, 2000);

        assertKeysNotInEventually(clusterC, "map", 0, 500);
        assertKeysNotInEventually(clusterC, "map", 1500, 2000);

        assertKeysInEventually(clusterC, "map", 500, 1500);

        removeDataIn(clusterA, "map", 500, 1000);
        removeDataIn(clusterB, "map", 1000, 1500);

        assertKeysNotInEventually(clusterC, "map", 0, 2000);
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
        assertDataInFromEventually(clusterC, "map", 0, 10, clusterA);

        createDataIn(clusterB, "map", 10, 20);
        assertDataInFromEventually(clusterC, "map", 10, 20, clusterB);

        assertKeysNotInEventually(clusterA, "map", 0, 10);
        assertKeysNotInEventually(clusterB, "map", 10, 20);
        assertKeysNotInEventually(clusterC, "map", 0, 20);
    }

    /**
     * Issue #1371 this topology was requested here https://groups.google.com/forum/#!msg/hazelcast/73jJo9W_v4A/5obqKMDQAnoJ
     */
    @Test
    public void VTopo_1activeActiveReplica_2producers_withPassThroughMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());

        setupReplicateFrom(configC, configA, clusterA.length, "ctoab", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configC, configB, clusterB.length, "ctoab", PassThroughMergePolicy.class.getName());

        startAllClusters();

        printAllReplicaConfig();

        createDataIn(clusterA, "map", 0, 100);
        createDataIn(clusterB, "map", 100, 200);

        assertDataInFromEventually(clusterC, "map", 0, 100, clusterA);
        assertDataInFromEventually(clusterC, "map", 100, 200, clusterB);

        assertDataInFromEventually(clusterA, "map", 100, 200, clusterB);
        assertDataInFromEventually(clusterB, "map", 0, 100, clusterA);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withPutIfAbsentMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", PutIfAbsentMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PutIfAbsentMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 100);
        createDataIn(clusterB, "map", 100, 200);

        assertDataInFromEventually(clusterC, "map", 0, 100, clusterA);
        assertDataInFromEventually(clusterC, "map", 100, 200, clusterB);

        createDataIn(clusterB, "map", 0, 100);
        assertDataInFromEventually(clusterC, "map", 0, 100, clusterA);

        assertDataSizeEventually(clusterC, "map", 200);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withLatestUpdateMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", LatestUpdateMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", LatestUpdateMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterC, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 0, 1000);
        assertDataInFromEventually(clusterC, "map", 0, 1000, clusterB);

        assertDataSizeEventually(clusterC, "map", 1000);

        removeDataIn(clusterA, "map", 0, 500);
        assertKeysNotInEventually(clusterC, "map", 0, 500);

        removeDataIn(clusterB, "map", 500, 1000);
        assertKeysNotInEventually(clusterC, "map", 500, 1000);

        assertDataSizeEventually(clusterC, "map", 0);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withHigherHitsMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitsMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFromEventually(clusterC, "map", 0, 10, clusterA);

        createDataIn(clusterB, "map", 0, 10);

        assertDataInFromEventually(clusterC, "map", 0, 10, clusterA);

        increaseHitCount(clusterB, "map", 0, 10, 100);
        createDataIn(clusterB, "map", 0, 10);

        assertDataInFromEventually(clusterC, "map", 0, 10, clusterB);
    }

    /**
     * Issue #1368 multi replica topology cluster A replicates to B and C
     */
    @Test
    public void VTopo_2passiveReplica_1producer() {
        String replicaName = "multiReplica";
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configC, clusterC.length, replicaName, PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertKeysInEventually(clusterC, "map", 0, 1000);

        removeDataIn(clusterA, "map", 0, 1000);

        assertKeysNotInEventually(clusterB, "map", 0, 1000);
        assertKeysNotInEventually(clusterC, "map", 0, 1000);

        assertDataSizeEventually(clusterB, "map", 0);
        assertDataSizeEventually(clusterC, "map", 0);
    }

    /**
     * Issue #1103
     */
    @Test
    public void multiBackupTest() {
        String replicaName = "multiBackup";
        configA.getMapConfig("default").setBackupCount(3);
        HazelcastInstance[] clusterA4Node = new HazelcastInstance[4];
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        initCluster(clusterA4Node, configA);
        startClusterB();

        createDataIn(clusterA4Node, "map", 0, 1000);
        assertKeysInEventually(clusterB, "map", 0, 1000);
        for (final HazelcastInstance instance : clusterA4Node) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    Map<String, LocalWanStats> stats
                            = getNode(instance).nodeEngine.getWanReplicationService().getStats();
                    LocalWanPublisherStats publisherStats =
                            stats.get("multiBackup").getLocalWanPublisherStats().get("B");
                    assert 0 == publisherStats.getOutboundQueueSize();
                }
            });
        }
    }

    @Test
    @Ignore(value = "see #linkTopo_ActiveActiveReplication_withThreading")
    public void linkTopo_ActiveActiveReplication() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 1000);
        assertDataInFromEventually(clusterB, "map", 0, 1000, clusterA);

        createDataIn(clusterB, "map", 1000, 2000);
        assertDataInFromEventually(clusterA, "map", 1000, 2000, clusterB);

        removeDataIn(clusterA, "map", 1500, 2000);
        assertKeysNotInEventually(clusterB, "map", 1500, 2000);

        removeDataIn(clusterB, "map", 0, 500);
        assertKeysNotInEventually(clusterA, "map", 0, 500);

        assertKeysInEventually(clusterA, "map", 500, 1500);
        assertKeysInEventually(clusterB, "map", 500, 1500);

        assertDataSizeEventually(clusterA, "map", 1000);
        assertDataSizeEventually(clusterB, "map", 1000);
    }

    @Test
    public void linkTopo_ActiveActiveReplication_2clusters_withHigherHitsMapMergePolicy() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", HigherHitsMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFromEventually(clusterB, "map", 0, 10, clusterA);

        increaseHitCount(clusterB, "map", 0, 5, 100);
        createDataIn(clusterB, "map", 0, 5);
        assertDataInFromEventually(clusterA, "map", 0, 5, clusterB);
    }

    @Test
    @Ignore(value = "same of replicationRing")
    public void chainTopo_2passiveReplicas_1producer() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 1000);

        assertKeysInEventually(clusterB, "map", 0, 1000);
        assertDataSizeEventually(clusterB, "map", 1000);

        assertKeysInEventually(clusterC, "map", 0, 1000);
        assertDataSizeEventually(clusterC, "map", 1000);
    }

    @Test
    public void wan_events_should_be_processed_in_order() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 10);
        removeAndCreateDataIn(clusterA, "map", 0, 10);

        assertKeysInEventually(clusterB, "map", 0, 10);
        assertDataSizeEventually(clusterB, "map", 10);
    }

    @Test
    public void replicationRing() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configC, configA, clusterA.length, "ctoa", PassThroughMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 10);

        assertKeysInEventually(clusterB, "map", 0, 10);
        assertDataSizeEventually(clusterB, "map", 10);

        assertKeysInEventually(clusterC, "map", 0, 10);
        assertDataSizeEventually(clusterC, "map", 10);
    }

    @Test
    public void linkTopo_ActiveActiveReplication_withThreading() throws Exception {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        final CountDownLatch putLatch = new CountDownLatch(2);
        CyclicBarrier gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            @Override
            public void go() {
                createDataIn(clusterA, "map", 0, 1000);
                putLatch.countDown();
            }
        });
        startGatedThread(new GatedThread(gate) {
            @Override
            public void go() {
                createDataIn(clusterB, "map", 500, 1500);
                putLatch.countDown();
            }
        });
        gate.await();

        // waiting for all puts to complete and the replication queues to drain in both clusters
        // needed to guarantee that puts happen before removes
        putLatch.await();
        assertOutboundQueueDrainedEventually(clusterA, "atob", "B");
        assertOutboundQueueDrainedEventually(clusterB, "btoa", "A");

        assertDataInFromEventually(clusterB, "map", 0, 500, clusterA);
        assertDataInFromEventually(clusterA, "map", 1000, 1500, clusterB);
        assertKeysInEventually(clusterA, "map", 500, 1000);

        gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            @Override
            public void go() {
                removeDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            @Override
            public void go() {
                removeDataIn(clusterB, "map", 500, 1500);
            }
        });
        gate.await();

        assertKeysNotInEventually(clusterA, "map", 0, 1500);
        assertKeysNotInEventually(clusterB, "map", 0, 1500);

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
        assertKeysNotInEventually(clusterB, "map", 0, 10);
    }

    @Test
    public void checkErasingMapMergePolicy() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", DeleteMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterB, "map", 0, 100);
        createDataIn(clusterA, "map", 0, 100);
        assertKeysNotInEventually(clusterB, "map", 0, 100);
    }

    @Test
    public void putAll() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        Map<Integer, Integer> inputMap = MapUtil.createHashMap(10);
        for (int i = 0; i < 10; i++) {
            inputMap.put(i, i);
        }
        IMap<Integer, Integer> map = getMap(clusterA, "map");
        map.putAll(inputMap);

        assertKeysInEventually(clusterB, "map", 0, 10);
    }

    @Test
    public void setTtl() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterB, "map", 0, 100);
        createDataIn(clusterA, "map", 0, 100);
        IMap<Integer, Integer> map = getMap(clusterA, "map");

        for (int i = 0; i < 100; i++) {
            map.setTtl(i, 1, TimeUnit.SECONDS);
        }

        assertKeysNotInEventually(clusterB, "map", 0, 100);
    }

    @Test
    public void setTtl_twoWay() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 100);
        createDataIn(clusterB, "map", 0, 100);
        IMap<Integer, Integer> mapA = getMap(clusterA, "map");
        IMap<Integer, Integer> mapB = getMap(clusterB, "map");

        for (int i = 0; i < 50; i++) {
            mapA.setTtl(i, 1, TimeUnit.SECONDS);
            mapB.setTtl(i + 50, 1, TimeUnit.SECONDS);
        }
        assertKeysNotInEventually(clusterB, "map", 0, 50);
        assertKeysNotInEventually(clusterA, "map", 50, 100);
    }

    @Test
    public void entryOperation() throws Exception {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        IMap<Integer, Integer> map = getMap(clusterA, "map");
        for (int i = 0; i < 20; i++) {
            map.put(i, i);
        }

        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapServiceContext mapServiceContext = ((MapService) mapProxy.getService()).getMapServiceContext();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapProxy.getName());

        InternalSerializationService serializationService = getSerializationService(clusterA[0]);
        Set<Data> keySet = new HashSet<Data>();
        for (int i = 0; i < 10; i++) {
            keySet.add(serializationService.toData(i));
        }

        // multiple entry operations
        OperationFactory operationFactory
                = operationProvider.createMultipleEntryOperationFactory(mapProxy.getName(), keySet, new UpdatingEntryProcessor());

        InternalOperationService operationService = getOperationService(clusterA[0]);
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, operationFactory);

        assertDataInFromEventually(clusterB, "map", 0, 10, "EP");

        OperationFactory deletingOperationFactory
                = operationProvider.createMultipleEntryOperationFactory(mapProxy.getName(), keySet, new DeletingEntryProcessor());
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, deletingOperationFactory);

        assertKeysNotInEventually(clusterB, "map", 0, 10);

        // entry operations
        IPartitionService partitionService = getPartitionService(clusterA[0]);

        MapOperation updatingEntryOperation = operationProvider.createEntryOperation(mapProxy.getName(),
                serializationService.toData(10), new UpdatingEntryProcessor());
        operationService.invokeOnPartition(MapService.SERVICE_NAME, updatingEntryOperation, partitionService.getPartitionId(10));

        assertDataInFromEventually(clusterB, "map", 10, 11, "EP");

        MapOperation deletingEntryOperation = operationProvider.createEntryOperation(mapProxy.getName(),
                serializationService.toData(10), new DeletingEntryProcessor());
        operationService.invokeOnPartition(MapService.SERVICE_NAME, deletingEntryOperation, partitionService.getPartitionId(10));

        assertKeysNotInEventually(clusterB, "map", 10, 11);
    }

    @Test
    public void putFromLoadAll() {
        // Used NoFilterMapWanFilter to override default behaviour of not publishing load events over WAN
        // which is introduced in version 3.11
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), NoFilterMapWanFilter.class.getName());

        MapConfig mapConfig = configA.getMapConfig("stored-map");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new SimpleStore());
        mapStoreConfig.setWriteDelaySeconds(0);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "stored-map", 0, 10);
        assertKeysInEventually(clusterB, "stored-map", 0, 10);

        getMap(clusterB, "stored-map").evictAll();
        assertKeysNotInEventually(clusterB, "store-map", 0, 10);

        IMap storedMap = getMap(clusterA, "stored-map");
        storedMap.loadAll(true);

        assertKeysInEventually(clusterB, "stored-map", 0, 10);
    }

    @Test
    public void putFromLoadAllAddsWanEventsOnAllReplicas() {
        final String setupName = "atob";
        // Used NoFilterMapWanFilter to override default behaviour of not publishing load events over WAN
        // which is introduced in version 3.11
        setupReplicateFrom(configA, configB, clusterB.length, setupName,
                PassThroughMergePolicy.class.getName(), NoFilterMapWanFilter.class.getName());
        final int startKey = 0;
        final int endKey = 10;

        final ConcurrentHashMap<Integer, String> initialStoreData = new ConcurrentHashMap<Integer, String>();
        for (int i = startKey; i < endKey; i++) {
            initialStoreData.put(i, "dummy");
        }

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(new SimpleStore<Integer, String>(initialStoreData))
                .setWriteDelaySeconds(0)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);
        configA.getMapConfig("stored-map").setMapStoreConfig(mapStoreConfig);

        startClusterA();
        getMap(clusterA, "stored-map").loadAll(true);
        assertWanQueueSizesEventually(clusterA, setupName, configB.getGroupConfig().getName(), 10);

        startClusterB();
        assertKeysInEventually(clusterB, "stored-map", startKey, endKey);
        assertWanQueueSizesEventually(clusterA, setupName, configB.getGroupConfig().getName(), 0);
    }

    @Test
    public void testStats() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", HigherHitsMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFromEventually(clusterB, "map", 0, 10, clusterA);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                WanReplicationService wanReplicationService = getNodeEngineImpl(clusterA[0]).getWanReplicationService();
                EnterpriseWanReplicationService ewrs = (EnterpriseWanReplicationService) wanReplicationService;
                assert ewrs.getStats().get("atob").getLocalWanPublisherStats().get("B").getOutboundQueueSize() == 0;
            }
        });
    }

    @Test
    public void testProxyCreation() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 0, 10);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Collection<DistributedObject> distributedObjects = clusterB[0].getDistributedObjects();
                assertEquals(1, distributedObjects.size());
            }
        }, 10);
        assertDataInFromEventually(clusterB, "map", 0, 10, clusterA);
    }

    @Test
    public void testStopResume() {
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 50);
        assertDataInFromEventually(clusterB, "map", 0, 50, clusterA);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        stopWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), WanPublisherState.STOPPED);
        createDataIn(clusterA, "map", 50, 100);
        assertKeysNotInEventually(clusterB, "map", 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), WanPublisherState.REPLICATING);
        createDataIn(clusterA, "map2", 0, 100);
        assertKeysInEventually(clusterB, "map2", 0, 100);
        assertKeysNotInEventually(clusterB, "map", 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testPauseResume() {
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 50);
        assertDataInFromEventually(clusterB, "map", 0, 50, clusterA);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        pauseWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.PAUSED);
        createDataIn(clusterA, "map", 50, 100);
        assertKeysNotInEventually(clusterB, "map", 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 50);

        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.REPLICATING);
        assertKeysInEventually(clusterB, "map", 0, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testPublisherInitialStateStopped() {
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName());

        final WanPublisherConfig targetClusterConfig = configA.getWanReplicationConfig("atob")
                                                              .getWanPublisherConfigs()
                                                              .get(0);
        targetClusterConfig.setInitialPublisherState(WanPublisherState.STOPPED);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 100);
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), WanPublisherState.STOPPED);
        assertKeysNotInEventually(clusterB, "map", 0, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), WanPublisherState.REPLICATING);
        createDataIn(clusterA, "map2", 0, 100);
        assertKeysInEventually(clusterB, "map2", 0, 100);
        assertKeysNotInEventually(clusterB, "map", 0, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testPublisherInitialStatePaused() {
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName());

        final WanPublisherConfig targetClusterConfig = configA.getWanReplicationConfig("atob")
                                                              .getWanPublisherConfigs()
                                                              .get(0);
        targetClusterConfig.setInitialPublisherState(WanPublisherState.PAUSED);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 100);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.PAUSED);
        assertKeysNotInEventually(clusterB, "map", 0, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 100);

        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.REPLICATING);
        assertKeysInEventually(clusterB, "map", 0, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    private static String getMBeanWanReplicationPublisherState(
            MBeanDataHolder mBeanDataHolder,
            String wanReplicationConfigName,
            String targetGroupName) throws Exception {
        return (String) mBeanDataHolder.getMBeanAttribute(
                "WanReplicationPublisher", wanReplicationConfigName + "." + targetGroupName, "state");
    }

    @Test
    public void wanPublisherStateIsVisibleViaJMX() throws Exception {
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName());
        configA.setProperty(GroupProperty.ENABLE_JMX.getName(), "true");
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 0, 50);

        final MBeanDataHolder mBeanDataHolder = new MBeanDataHolder(clusterA[0]);
        assertEquals(WanPublisherState.REPLICATING.name(),
                getMBeanWanReplicationPublisherState(mBeanDataHolder, wanReplicationConfigName, targetGroupName));

        stopWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), WanPublisherState.STOPPED);

        assertEquals(WanPublisherState.STOPPED.name(),
                getMBeanWanReplicationPublisherState(mBeanDataHolder, wanReplicationConfigName, targetGroupName));
    }

    @Test
    public void replicated_data_is_not_persisted_by_default() {
        final int mapEntryCount = 1001;
        String mapName = "default";

        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName());

        // 2. Add map-store to cluster-B
        final TestMapStore store = new TestMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(store);
        configB.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        // 3. Start cluster-A and cluster-B
        startClusterA();
        startClusterB();

        // 4. Add entries from cluster-A
        IMap mapA = getMap(clusterA, mapName);
        for (int i = 0; i < mapEntryCount; i++) {
            mapA.put(i, i);
        }

        // 5. Delete entries from cluster-A
        for (int i = 0; i < mapEntryCount; i++) {
            mapA.delete(i);
        }

        // 6. Ensure no put or delete operation is passed to map-store
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, store.storeCount.get());
                assertEquals(0, store.deleteCount.get());
            }
        }, 5);
    }

    @Test
    public void replicated_data_is_persisted_when_persistWanReplicatedData_is_true() {
        final int mapEntryCount = 1001;
        String mapName = "default";

        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName());

        // 2. Add map-store to cluster-B
        final TestMapStore store = new TestMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(store);
        MapConfig mapConfig = configB.getMapConfig(mapName);
        mapConfig.setWanReplicationRef(getWanReplicationRefFrom(configB, true));
        mapConfig.setMapStoreConfig(mapStoreConfig);

        // 3. Start cluster-A and cluster-B
        startClusterA();
        startClusterB();

        // 4. Add entries from cluster-A
        IMap mapA = getMap(clusterA, mapName);
        for (int i = 0; i < mapEntryCount; i++) {
            mapA.put(i, i);
        }

        // 5. Delete entries from cluster-A
        for (int i = 0; i < mapEntryCount; i++) {
            mapA.delete(i);
        }

        // 6. Ensure all put and delete operations are passed to map-store
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(mapEntryCount, store.storeCount.get());
                assertEquals(mapEntryCount, store.deleteCount.get());
            }
        });
    }

    @Test
    public void maxIdleFromTargetClusterIsUsedForReceivedEntries() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName());
        configA.getMapConfig("default").setMaxIdleSeconds(1);
        configA.setProperty(CLEANUP_PERCENTAGE.getName(), "100")
               .setProperty(TASK_PERIOD_SECONDS.getName(), "1");
        startClusterA();
        startClusterB();

        final String mapName = "map";
        createDataIn(clusterA, mapName, 0, 100);
        assertDataInFromEventually(clusterB, mapName, 0, 100, clusterA);

        assertSizeEventually(0, clusterA[0].getMap(mapName));
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertDataInFrom(clusterB, mapName, 0, 100, clusterA);
            }
        }, 10);
    }

    @Test
    public void publisherIdOverridesGroupName() {
        configB.getGroupConfig().setName("targetGroup");
        configC.getGroupConfig().setName("targetGroup");
        String wanReplicationScheme = "replicationScheme";

        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationScheme, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configC, clusterB.length, wanReplicationScheme, PassThroughMergePolicy.class.getName());

        List<WanPublisherConfig> publisherConfigs = configA.getWanReplicationConfig(wanReplicationScheme)
                                                           .getWanPublisherConfigs();
        assertEquals(2, publisherConfigs.size());
        publisherConfigs.get(0).setPublisherId("publisher1");
        publisherConfigs.get(1).setPublisherId("publisher2");

        initCluster(singleNodeA, configA);
        createDataIn(singleNodeA, "map", 0, 100);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void duplicatePublisherConfigThrowsException() {
        String wanReplicationScheme = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationScheme, PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationScheme, PassThroughMergePolicy.class.getName());

        for (WanPublisherConfig publisherConfig : configA.getWanReplicationConfig(wanReplicationScheme)
                                                         .getWanPublisherConfigs()) {
            publisherConfig.setClassName(UninitializableWanEndpoint.class.getName());
        }

        startClusterA();

        clusterA[0].getMap("map")
                   .put(1, 1);
    }

    private void assertOutboundQueueDrainedEventually(final HazelcastInstance[] cluster, final String wanPublisherName,
                                                      final String targetGroupName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : cluster) {
                    WanReplicationService wanReplicationService = getNodeEngineImpl(instance).getWanReplicationService();
                    EnterpriseWanReplicationService ewrs = (EnterpriseWanReplicationService) wanReplicationService;
                    assert ewrs.getStats().get(wanPublisherName).getLocalWanPublisherStats().get(targetGroupName)
                               .getOutboundQueueSize() == 0;
                }
            }
        });
    }

    private static WanReplicationRef getWanReplicationRefFrom(Config config,
                                                              boolean persistWanReplicatedData) {

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("b");

        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        consumerConfig.setPersistWanReplicatedData(persistWanReplicatedData);
        consumerConfig.setClassName(CustomWanConsumer.class.getName());
        wanReplicationConfig.setWanConsumerConfig(consumerConfig);
        config.addWanReplicationConfig(wanReplicationConfig);

        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        wanReplicationRef.setName("b");
        wanReplicationRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        return wanReplicationRef;
    }

    private class TestMapStore extends MapStoreAdapter {

        AtomicInteger storeCount = new AtomicInteger();
        AtomicInteger deleteCount = new AtomicInteger();

        @Override
        public void store(Object key, Object value) {
            storeCount.incrementAndGet();
        }

        @Override
        public void delete(Object key) {
            deleteCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "TestMapStore{"
                    + "storeCount=" + storeCount
                    + ", deleteCount=" + deleteCount
                    + '}';
        }
    }

    private static class UpdatingEntryProcessor implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            entry.setValue("EP" + entry.getValue());
            return "done";
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<Object, Object> entry) {
            process(entry);
        }
    }

    private static class DeletingEntryProcessor implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            entry.setValue(null);
            return "done";
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<Object, Object> entry) {
            process(entry);
        }
    }

    private static class SimpleStore<K, V> implements MapStore<K, V> {

        private final ConcurrentMap<K, V> store;

        SimpleStore() {
            this(new ConcurrentHashMap<K, V>());
        }

        SimpleStore(ConcurrentMap<K, V> store) {
            this.store = store;
        }

        @Override
        public void store(K key, V value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<K, V> map) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(K key) {
        }

        @Override
        public void deleteAll(Collection keys) {
        }

        @Override
        public V load(K key) {
            return store.get(key);
        }

        @Override
        public Map<K, V> loadAll(Collection<K> keys) {
            Map<K, V> map = new HashMap<K, V>();
            for (K key : keys) {
                V value = load(key);
                map.put(key, value);
            }
            return map;
        }

        @Override
        public Set<K> loadAllKeys() {
            return store.keySet();
        }
    }
}
