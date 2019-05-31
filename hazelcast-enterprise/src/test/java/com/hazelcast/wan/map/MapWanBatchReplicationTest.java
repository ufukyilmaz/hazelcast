package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
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
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.internal.jmx.MBeanDataHolder;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapLoaderTest;
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
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.UninitializableWanEndpoint;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.WanSyncStatus;
import com.hazelcast.wan.map.filter.DummyMapWanFilter;
import com.hazelcast.wan.map.filter.NoFilterMapWanFilter;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

import static com.hazelcast.config.WanPublisherState.PAUSED;
import static com.hazelcast.config.WanPublisherState.STOPPED;
import static com.hazelcast.enterprise.wan.replication.WanReplicationProperties.ENDPOINTS;
import static com.hazelcast.map.impl.EnterpriseMapReplicationSupportingService.PROP_USE_DELETE_WHEN_PROCESSING_REMOVE_EVENTS;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class MapWanBatchReplicationTest extends MapWanReplicationTestSupport {

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public int maxConcurrentInvocations;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, maxConcurrentInvocations:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, -1},
                {InMemoryFormat.BINARY, 100},
                {InMemoryFormat.OBJECT, -1},
                {InMemoryFormat.NATIVE, -1},
        });
    }

    @Override
    public final InMemoryFormat getMemoryFormat() {
        return inMemoryFormat;
    }

    @Override
    protected int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        config.getMapConfig("default")
                .setInMemoryFormat(getMemoryFormat());

        return config;
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
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
        assertDataInFromEventually(clusterB, "map", 0, 1000, getNode(clusterA[1]).getConfig().getGroupConfig().getName());
    }

    @Test
    public void testMapWanFilter() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), DummyMapWanFilter.class.getName());
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 1, 10);
        assertKeysInEventually(clusterB, "map", 1, 2);
        assertKeysNotInEventually(clusterB, "map", 2, 10);
    }

    @Test
    public void mapWanEventFilter_prevents_replication_of_loaded_entries_by_default() {
        final String mapName = "default";
        final int loadedEntryCount = 1111;

        // 1. MapWanEventFilter is null to see default behaviour of filtering
        setupReplicateFrom(configA, configB, singleNodeB.length, "atob",
                PassThroughMergePolicy.class.getName());

        // 2. Ensure WAN events are enqueued but not replicated
        // PAUSED state let WAN events offered to the queues, but prevents replicating it
        configA.getWanReplicationConfig("atob")
                .getWanPublisherConfigs().get(0).setInitialPublisherState(PAUSED);

        // 3. Add map-loader to cluster-A
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setImplementation(new MapLoaderTest.DummyMapLoader(loadedEntryCount));
        configA.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        // 4. Start single-node cluster-A and single-node cluster-B
        initCluster(singleNodeA, configA);
        initCluster(singleNodeB, configB);

        // 5. Create map to trigger eager map-loading on cluster-A
        getMap(singleNodeA, mapName);

        // Getting the outbound queue counter directly from the WanBatchReplication instance, which is synchronously updated
        // on the partition operation thread
        int outboundQueueSize = getOutboundQueueSizeFromReplicationInstance(singleNodeA[0], "atob", "B");

        // 5. Ensure no keys are replicated to cluster-B
        assertEquals("Loading from MapLoader should not trigger WAN publication", 0, outboundQueueSize);
    }

    private int getOutboundQueueSizeFromReplicationInstance(HazelcastInstance instance, String setupName, String publisherName) {
        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(instance);
        WanReplicationPublisherDelegate publisherDelegate = (WanReplicationPublisherDelegate) wanReplicationService
                .getWanReplicationPublisher(setupName);
        WanBatchReplication wanBatchReplication = (WanBatchReplication) publisherDelegate.getEndpoint(publisherName);
        return wanBatchReplication.getCurrentElementCount();
    }

    @Test
    public void mapWanEventFilter_allows_replication_of_loaded_entries_when_customized() {
        String mapName = "default";
        int loadedEntryCount = 1111;

        // 1. Add customized MapWanEventFilter. This filter doesn't do any filtering.
        setupReplicateFrom(configA, configB, singleNodeB.length, "atob",
                PassThroughMergePolicy.class.getName(), NoFilterMapWanFilter.class.getName());

        // 2. Add map-loader to cluster-A
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setImplementation(new MapLoaderTest.DummyMapLoader(loadedEntryCount));
        configA.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        // 3. Start single-node cluster-A and single-node cluster-B
        initCluster(singleNodeA, configA);
        initCluster(singleNodeB, configB);

        // 4. Create map to trigger eager map-loading on cluster-A
        getMap(singleNodeA, mapName);

        // 5. Ensure all keys are replicated to cluster-B eventually.
        assertKeysInEventually(singleNodeB, mapName, 0, loadedEntryCount);
    }

    @Test
    public void testMigration() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());

        initCluster(singleNodeA, configA);
        createDataIn(singleNodeA, "map", 0, 1000);
        initCluster(singleNodeC, configA);

        initCluster(clusterB, configB);

        assertDataInFromEventually(clusterB, "map", 0, 1000, singleNodeC[0].getConfig().getGroupConfig().getName());
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
        assumeTrue("maxConcurrentInvocations higher than 1 does not guarantee ordering", maxConcurrentInvocations < 2);
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
        assertWanQueueSizesEventually(clusterA, "atob", "B", 0);
        assertWanQueueSizesEventually(clusterB, "btoa", "A", 0);

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
        assertWanQueueSizesEventually(clusterA, "atob", configB.getGroupConfig().getName(), 0);
    }

    @Test
    public void setTtl() {
        String mergePolicy = com.hazelcast.spi.merge.PassThroughMergePolicy.class.getName();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", mergePolicy);
        startClusterA();
        startClusterB();

        createDataIn(clusterB, "map", 0, 100, "value");
        createDataIn(clusterA, "map", 0, 100, "value");
        IMap<Integer, Integer> map = getMap(clusterA, "map");

        assertWanQueueSizesEventually(clusterA, "atob", configB.getGroupConfig().getName(), 0);

        for (int i = 0; i < 100; i++) {
            map.setTtl(i, 1, TimeUnit.SECONDS);
        }

        assertKeysNotInEventually(clusterB, "map", 0, 100);
    }

    @Test
    public void setTtl_twoWay() {
        String mergePolicy = com.hazelcast.spi.merge.PassThroughMergePolicy.class.getName();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", mergePolicy);
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", mergePolicy);
        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 100, "value");
        createDataIn(clusterB, "map", 0, 100, "value");
        IMap<Integer, Integer> mapA = getMap(clusterA, "map");
        IMap<Integer, Integer> mapB = getMap(clusterB, "map");

        assertWanQueueSizesEventually(clusterA, "atob", configB.getGroupConfig().getName(), 0);
        assertWanQueueSizesEventually(clusterB, "btoa", configA.getGroupConfig().getName(), 0);

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

        assertKeysInEventually(clusterB, "map", 0, 10);

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

        OperationService operationService = getOperationService(clusterA[0]);
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
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), STOPPED);
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
        targetClusterConfig.setInitialPublisherState(STOPPED);

        startClusterA();
        startClusterB();

        createDataIn(clusterA, "map", 0, 100);
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), STOPPED);
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
        assertWanPublisherStateEventually(clusterA, "atob", configB.getGroupConfig().getName(), STOPPED);

        assertEquals(STOPPED.name(),
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

        setUseMapDeletePropertyIfReplicationConcurrent(configB, maxConcurrentInvocations);

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
                assertEquals("unexpected store count", 0, store.storeCount.get());
                assertEquals("unexpected delete count", 0, store.deleteCount.get());
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

        setUseMapDeletePropertyIfReplicationConcurrent(configB, maxConcurrentInvocations);

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
                assertEquals("missing store operations", mapEntryCount, store.storeCount.get());
                assertEquals("missing delete operations", mapEntryCount, store.deleteCount.get());
            }
        });
    }

    /**
     * Use map#delete to process events on target cluster when
     * maxConcurrentInvocations > 1 because, in contrast with
     * map#remove, map#delete always calls mapstore#delete
     * without pre-checking existence of key.
     *
     * Below setting is needed due to the out of order nature of
     * wan concurrent invocations. In this test updates can be
     * reordered with removes when maxConcurrentInvocations > 1.
     */
    private static void setUseMapDeletePropertyIfReplicationConcurrent(Config config,
                                                                       int maxConcurrentInvocations) {
        if (maxConcurrentInvocations > 1) {
            config.setProperty(PROP_USE_DELETE_WHEN_PROCESSING_REMOVE_EVENTS, "true");
        }
    }

    @Test
    public void maxIdleFromTargetClusterIsUsedForReceivedEntries() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName());
        configA.getMapConfig("default").setMaxIdleSeconds(2);
        configA.setProperty(PROP_CLEANUP_PERCENTAGE, "100")
                .setProperty(PROP_TASK_PERIOD_SECONDS, "1");
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

    private static WanReplicationRef getWanReplicationRefFrom(Config config, boolean persistWanReplicatedData) {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("b");

        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        consumerConfig.setPersistWanReplicatedData(persistWanReplicatedData);

        wanReplicationConfig.setWanConsumerConfig(consumerConfig);

        config.addWanReplicationConfig(wanReplicationConfig);

        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        wanReplicationRef.setName("b");
        wanReplicationRef.setMergePolicy(PassThroughMergePolicy.class.getName());
        return wanReplicationRef;
    }

    static void waitForSyncToComplete(final HazelcastInstance[] cluster) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean syncFinished = true;
                for (HazelcastInstance instance : cluster) {
                    syncFinished &= wanReplicationService(instance).getWanSyncState().getStatus() != WanSyncStatus.IN_PROGRESS;
                }
                assertTrue(syncFinished);
            }
        });
    }

    public static boolean isAllMembersConnected(HazelcastInstance[] cluster, String setupName, String publisherId) {
        boolean allConnected = true;
        for (HazelcastInstance instance : cluster) {
            allConnected &= checkIfConnected(instance, setupName, publisherId);
        }
        return allConnected;
    }

    private static boolean checkIfConnected(HazelcastInstance instance, String setupName, String publisherId) {
        return wanReplicationService(instance)
                .getStats()
                .get(setupName).getLocalWanPublisherStats()
                .get(publisherId).isConnected();
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
