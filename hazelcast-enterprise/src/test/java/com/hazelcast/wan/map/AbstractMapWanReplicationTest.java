package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
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
import com.hazelcast.wan.WanReplicationService;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertEquals;

public abstract class AbstractMapWanReplicationTest extends MapWanReplicationTestSupport {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true");

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
        assertDataInFrom(clusterC, "map", 0, 10, clusterA);

        createDataIn(clusterB, "map", 10, 20);
        assertDataInFrom(clusterC, "map", 10, 20, clusterB);

        sleepSeconds(20);
        assertKeysNotIn(clusterA, "map", 0, 10);
        assertKeysNotIn(clusterB, "map", 10, 20);
        assertKeysNotIn(clusterC, "map", 0, 20);
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

        assertDataInFrom(clusterC, "map", 0, 100, clusterA);
        assertDataInFrom(clusterC, "map", 100, 200, clusterB);

        assertDataInFrom(clusterA, "map", 100, 200, clusterB);
        assertDataInFrom(clusterB, "map", 0, 100, clusterA);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withPutIfAbsentMapMergePolicy() {
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
    public void VTopo_1passiveReplica_2producers_withLatestUpdateMapMergePolicy() {
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
    public void VTopo_1passiveReplica_2producers_withHigherHitsMapMergePolicy() {
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitsMapMergePolicy.class.getName());
        startAllClusters();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFrom(clusterC, "map", 0, 10, clusterA);

        createDataIn(clusterB, "map", 0, 10);

        assertDataInFrom(clusterC, "map", 0, 10, clusterA);

        increaseHitCount(clusterB, "map", 0, 10, 100);
        createDataIn(clusterB, "map", 0, 10);

        assertDataInFrom(clusterC, "map", 0, 10, clusterB);
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

        assertKeysIn(clusterB, "map", 0, 1000);
        assertKeysIn(clusterC, "map", 0, 1000);

        removeDataIn(clusterA, "map", 0, 1000);

        assertKeysNotIn(clusterB, "map", 0, 1000);
        assertKeysNotIn(clusterC, "map", 0, 1000);

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
        assertKeysIn(clusterB, "map", 0, 1000);
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
    public void linkTopo_ActiveActiveReplication_2clusters_withHigherHitsMapMergePolicy() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", HigherHitsMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFrom(clusterB, "map", 0, 10, clusterA);

        increaseHitCount(clusterB, "map", 0, 5, 100);
        createDataIn(clusterB, "map", 0, 5);
        assertDataInFrom(clusterA, "map", 0, 5, clusterB);
        sleepSeconds(10);
    }

    @Test
    @Ignore(value = "same of replicationRing")
    public void chainTopo_2passiveReplicas_1producer() {
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

        createDataIn(clusterA, "map", 0, 10);

        assertKeysIn(clusterB, "map", 0, 10);
        assertDataSizeEventually(clusterB, "map", 10);

        assertKeysIn(clusterC, "map", 0, 10);
        assertDataSizeEventually(clusterC, "map", 10);
    }

    @Test
    public void linkTopo_ActiveActiveReplication_withThreading() throws Exception {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        CyclicBarrier gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            @Override
            public void go() {
                createDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            @Override
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

        assertKeysIn(clusterB, "map", 0, 10);
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

        assertDataInFrom(clusterB, "map", 0, 10, "EP");

        OperationFactory deletingOperationFactory
                = operationProvider.createMultipleEntryOperationFactory(mapProxy.getName(), keySet, new DeletingEntryProcessor());
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, deletingOperationFactory);

        assertKeysNotIn(clusterB, "map", 0, 10);

        // entry operations
        IPartitionService partitionService = getPartitionService(clusterA[0]);

        MapOperation updatingEntryOperation = operationProvider.createEntryOperation(mapProxy.getName(),
                serializationService.toData(10), new UpdatingEntryProcessor());
        operationService.invokeOnPartition(MapService.SERVICE_NAME, updatingEntryOperation, partitionService.getPartitionId(10));

        checkDataInFrom(clusterB, "map", 10, 11, "EP");

        MapOperation deletingEntryOperation = operationProvider.createEntryOperation(mapProxy.getName(),
                serializationService.toData(10), new DeletingEntryProcessor());
        operationService.invokeOnPartition(MapService.SERVICE_NAME, deletingEntryOperation, partitionService.getPartitionId(10));

        assertKeysNotIn(clusterB, "map", 10, 11);
    }

    @Test
    public void putFromLoadAll() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = configA.getMapConfig("stored-map");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new SimpleStore());
        mapStoreConfig.setWriteDelaySeconds(0);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "stored-map", 0, 10);
        assertKeysIn(clusterB, "stored-map", 0, 10);

        getMap(clusterB, "stored-map").evictAll();
        assertKeysNotIn(clusterB, "store-map", 0, 10);

        IMap storedMap = getMap(clusterA, "stored-map");
        storedMap.loadAll(true);

        assertKeysIn(clusterB, "stored-map", 0, 10);
    }

    @Test
    public void putFromLoadAllAddsWanEventsOnAllReplicas() {
        final String setupName = "atob";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());
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
        assertWanQueueSizesOnAllInstances(clusterA, setupName, configB.getGroupConfig().getName(), 10);

        startClusterB();
        assertKeysIn(clusterB, "stored-map", startKey, endKey);
        assertWanQueueSizesOnAllInstances(clusterA, setupName, configB.getGroupConfig().getName(), 0);
    }

    @Test
    public void testStats() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMapMergePolicy.class.getName());
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", HigherHitsMapMergePolicy.class.getName());
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 10);
        assertDataInFrom(clusterB, "map", 0, 10, clusterA);

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
        assertDataInFrom(clusterB, "map", 0, 10, clusterA);
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

    private static void assertWanQueueSizesOnAllInstances(final HazelcastInstance[] cluster,
                                                          final String wanReplicationConfigName,
                                                          final String endpointGroupName,
                                                          final int eventCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int totalBackupEvents = 0;
                int totalEvents = 0;
                for (HazelcastInstance instance : cluster) {
                    final EnterpriseWanReplicationService s
                            = getNode(instance).nodeEngine.getService(EnterpriseWanReplicationService.SERVICE_NAME);
                    final WanReplicationPublisherDelegate delegate
                            = (WanReplicationPublisherDelegate) s.getWanReplicationPublisher(wanReplicationConfigName);
                    final AbstractWanPublisher endpoint = (AbstractWanPublisher) delegate.getEndpoint(endpointGroupName);
                    totalEvents += endpoint.getCurrentElementCount();
                    totalBackupEvents += endpoint.getCurrentBackupElementCount();
                }
                assertEquals(eventCount, totalEvents);
                assertEquals(eventCount, totalBackupEvents);
            }
        });
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
