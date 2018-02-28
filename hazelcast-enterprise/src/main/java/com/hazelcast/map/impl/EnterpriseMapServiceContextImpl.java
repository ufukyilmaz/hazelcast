package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.event.EnterpriseMapEventPublisherImpl;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.nearcache.EnterpriseMapNearCacheManager;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.EnterpriseMapOperationProviders;
import com.hazelcast.map.impl.operation.EnterpriseMapPartitionClearOperation;
import com.hazelcast.map.impl.operation.HDBasePutOperation;
import com.hazelcast.map.impl.operation.HDBaseRemoveOperation;
import com.hazelcast.map.impl.operation.HDGetOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperationProviders;
import com.hazelcast.map.impl.query.HDIndexProvider;
import com.hazelcast.map.impl.query.HDPartitionScanExecutor;
import com.hazelcast.map.impl.query.HDPartitionScanRunner;
import com.hazelcast.map.impl.query.IndexProvider;
import com.hazelcast.map.impl.query.PartitionScanExecutor;
import com.hazelcast.map.impl.query.PartitionScanRunner;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultProcessorRegistry;
import com.hazelcast.map.impl.record.HDRecordComparator;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.wan.filter.MapFilterProvider;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.PersistentConfigDescriptors;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.lang.Thread.currentThread;

/**
 * Contains enterprise specific implementations of {@link MapServiceContext}
 * functionality.
 *
 * @see MapServiceContext
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
class EnterpriseMapServiceContextImpl extends MapServiceContextImpl implements EnterpriseMapServiceContext, RamStoreRegistry {

    private static final int MAP_PARTITION_CLEAR_OPERATION_AWAIT_TIME_IN_SECS = 10;

    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        @Override
        public MapContainer createNew(String mapName) {
            MapServiceContext mapServiceContext = getService().getMapServiceContext();
            Config config = mapServiceContext.getNodeEngine().getConfig();
            return new EnterpriseMapContainer(mapName, config, mapServiceContext);
        }
    };

    private final QueryRunner hdMapQueryRunner;
    private final HDPartitionScanRunner hdPartitionScanRunner;
    private final MapFilterProvider mapFilterProvider;
    private final HDIndexProvider hdIndexProvider;

    private HotRestartIntegrationService hotRestartService;

    EnterpriseMapServiceContextImpl(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.mapFilterProvider = new MapFilterProvider(nodeEngine);
        Node node = ((NodeEngineImpl) nodeEngine).getNode();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        if (nodeExtension.isHotRestartEnabled()) {
            hotRestartService = (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService();
            hotRestartService.registerRamStoreRegistry(MapService.SERVICE_NAME, this);
        }
        this.hdPartitionScanRunner = new HDPartitionScanRunner(this);
        this.hdMapQueryRunner = createHDMapQueryRunner(hdPartitionScanRunner, getQueryOptimizer(),
                getResultProcessorRegistry());
        this.hdIndexProvider = new HDIndexProvider();
    }

    @Override
    MapOperationProviders createOperationProviders() {
        return new EnterpriseMapOperationProviders(this);
    }

    @Override
    void initRecordComparators() {
        super.initRecordComparators();
        recordComparatorMap.put(InMemoryFormat.NATIVE, new HDRecordComparator(serializationService));
    }

    @Override
    public HotRestartStore getOnHeapHotRestartStoreForPartition(int partitionId) {
        return hotRestartService.getOnHeapHotRestartStoreForPartition(partitionId);
    }

    @Override
    public HotRestartStore getOffHeapHotRestartStoreForPartition(int partitionId) {
        return hotRestartService.getOffHeapHotRestartStoreForPartition(partitionId);
    }

    @Override
    public RamStore ramStoreForPrefix(long prefix) {
        int partitionId = PersistentConfigDescriptors.toPartitionId(prefix);
        String name = hotRestartService.getCacheName(prefix);
        EnterpriseRecordStore recordStore = (EnterpriseRecordStore) getExistingRecordStore(partitionId, name);
        return recordStore == null ? null : recordStore.getRamStore();
    }

    @Override
    public RamStore restartingRamStoreForPrefix(long prefix) {
        int partitionId = PersistentConfigDescriptors.toPartitionId(prefix);
        String name = hotRestartService.getCacheName(prefix);
        PartitionContainer partitionContainer = getPartitionContainer(partitionId);
        EnterpriseRecordStore recordStore = (EnterpriseRecordStore) partitionContainer.getRecordStoreForHotRestart(name);
        return recordStore.getRamStore();
    }

    @Override
    public int prefixToThreadId(long prefix) {
        throw new UnsupportedOperationException("prefixToThreadId() should not have been called on this class");
    }

    @Override
    MapNearCacheManager createMapNearCacheManager() {
        return new EnterpriseMapNearCacheManager(this);
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, contextMutexFactory, mapConstructor);
    }

    @Override
    public MapFilterProvider getMapFilterProvider() {
        return mapFilterProvider;
    }

    @Override
    public PartitionScanRunner getPartitionScanRunner() {
        return hdPartitionScanRunner;
    }

    @Override
    public QueryRunner getMapQueryRunner(String mapName) {
        if (getInMemoryFormat(mapName) != NATIVE) {
            return super.getMapQueryRunner(mapName);
        }
        return hdMapQueryRunner;
    }

    private QueryRunner createHDMapQueryRunner(HDPartitionScanRunner runner, QueryOptimizer queryOptimizer,
                                               ResultProcessorRegistry resultProcessorRegistry) {
        PartitionScanExecutor partitionScanExecutor = new HDPartitionScanExecutor(runner);
        return new QueryRunner(this, queryOptimizer, partitionScanExecutor, resultProcessorRegistry);
    }

    @Override
    MapEventPublisherImpl createMapEventPublisherSupport() {
        return new EnterpriseMapEventPublisherImpl(this);
    }

    @Override
    public MapOperationProvider getMapOperationProvider(String name) {
        return operationProviders.getOperationProvider(name);
    }

    @Override
    public Data toData(Object object) {
        return ((EnterpriseSerializationService) getNodeEngine().getSerializationService()).toData(object, DataType.HEAP);
    }

    @Override
    public void clearMapsHavingLesserBackupCountThan(int partitionId, int backupCount) {
        PartitionContainer container = getPartitionContainer(partitionId);
        if (container != null) {
            Iterator<RecordStore> iter = container.getMaps().values().iterator();
            while (iter.hasNext()) {
                RecordStore recordStore = iter.next();
                if (backupCount > recordStore.getMapContainer().getTotalBackupCount()) {
                    recordStore.destroy();
                    iter.remove();
                }
            }
        }
    }

    @Override
    public void clearPartitionData(int partitionId) {
        PartitionContainer container = partitionContainers[partitionId];
        if (container != null) {
            for (RecordStore recordStore : container.getMaps().values()) {
                recordStore.destroy();
            }
            container.getMaps().clear();
        }
    }

    @Override
    public void clearPartitions(boolean onShutdown) {
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();

        List<EnterpriseMapPartitionClearOperation> operations = new ArrayList<EnterpriseMapPartitionClearOperation>();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer partitionContainer = getPartitionContainer(i);
            ConcurrentMap<String, RecordStore> maps = partitionContainer.getMaps();
            if (maps.isEmpty()) {
                continue;
            }

            EnterpriseMapPartitionClearOperation operation = new EnterpriseMapPartitionClearOperation(onShutdown);
            operation.setPartitionId(i)
                    .setNodeEngine(nodeEngine)
                    .setService(getService());

            if (operationService.isRunAllowed(operation)) {
                operationService.run(operation);
            } else {
                operationService.execute(operation);
                operations.add(operation);
            }
        }

        for (EnterpriseMapPartitionClearOperation operation : operations) {
            try {
                operation.awaitCompletion(MAP_PARTITION_CLEAR_OPERATION_AWAIT_TIME_IN_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }
    }

    @Override
    public void incrementOperationStats(long startTime, LocalMapStatsImpl localMapStats, String mapName, Operation operation) {
        if (getInMemoryFormat(mapName) != NATIVE) {
            super.incrementOperationStats(startTime, localMapStats, mapName, operation);
        }

        if (operation instanceof HDBasePutOperation) {
            localMapStats.incrementPutLatencyNanos(System.nanoTime() - startTime);
            return;
        }

        if (operation instanceof HDBaseRemoveOperation) {
            localMapStats.incrementRemoveLatencyNanos(System.nanoTime() - startTime);
            return;
        }

        if (operation instanceof HDGetOperation) {
            localMapStats.incrementGetLatencyNanos(System.nanoTime() - startTime);
        }
    }

    protected InMemoryFormat getInMemoryFormat(String mapName) {
        MapContainer container = getMapContainer(mapName);
        MapConfig mapConfig = container.getMapConfig();
        return mapConfig.getInMemoryFormat();
    }

    @Override
    public RecordStore createRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader) {
        ILogger logger = nodeEngine.getLogger(DefaultRecordStore.class);
        long prefix = -1;
        HotRestartConfig hotRestartConfig = getHotRestartConfig(mapContainer);
        if (hotRestartConfig.isEnabled()) {
            if (hotRestartService == null) {
                throw new HazelcastException("Hot Restart is enabled for map: " + mapContainer.getMapConfig().getName()
                        + " but Hot Restart persistence is not enabled!");
            }
            String name = mapContainer.getName();
            hotRestartService.ensureHasConfiguration(MapService.SERVICE_NAME, name, null);
            prefix = hotRestartService.registerRamStore(this, MapService.SERVICE_NAME, name, partitionId);

            ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
            proxyService.getOrCreateRegistry(MapService.SERVICE_NAME).getOrCreateProxyFuture(name, false, false);
        }
        return new EnterpriseRecordStore(mapContainer, partitionId, keyLoader, logger, hotRestartConfig, prefix);
    }

    private HotRestartConfig getHotRestartConfig(MapContainer mapContainer) {
        HotRestartConfig hotRestartConfig = mapContainer.getMapConfig().getHotRestartConfig();
        return hotRestartConfig != null ? hotRestartConfig : new HotRestartConfig().setEnabled(false);
    }

    @Override
    public IndexProvider getIndexProvider(MapConfig mapConfig) {
        if (mapConfig.getInMemoryFormat() != NATIVE) {
            return super.getIndexProvider(mapConfig);
        }
        return hdIndexProvider;
    }
}
