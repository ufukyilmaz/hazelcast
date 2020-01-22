package com.hazelcast.map.impl;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.hotrestart.HotRestartIntegrationService;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.PersistentConfigDescriptors;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.hotrestart.RamStoreRegistry;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.util.comparators.NativeValueComparator;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.event.EnterpriseMapEventPublisherImpl;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.nearcache.EnterpriseMapNearCacheManager;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.EnterpriseMapPartitionClearOperation;
import com.hazelcast.map.impl.query.HDPartitionScanExecutor;
import com.hazelcast.map.impl.query.HDPartitionScanRunner;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultProcessorRegistry;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.wan.MapFilterProvider;
import com.hazelcast.query.impl.HDIndexProvider;
import com.hazelcast.query.impl.IndexProvider;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_PREFIX;
import static java.lang.Thread.currentThread;

/**
 * Contains enterprise specific implementations
 * of {@link MapServiceContext} functionality.
 *
 * @see MapServiceContext
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
class EnterpriseMapServiceContextImpl extends MapServiceContextImpl
        implements EnterpriseMapServiceContext, RamStoreRegistry {

    private static final int MAP_PARTITION_CLEAR_OPERATION_AWAIT_SECONDS = 10;

    private final QueryRunner hdMapQueryRunner;
    private final HDIndexProvider hdIndexProvider;
    private final MapFilterProvider mapFilterProvider;

    private HotRestartIntegrationService hotRestartService;

    EnterpriseMapServiceContextImpl(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.mapFilterProvider = new MapFilterProvider(nodeEngine);
        this.hdMapQueryRunner = createHDMapQueryRunner(new HDPartitionScanRunner(this),
                getQueryOptimizer(), getResultProcessorRegistry());
        this.hdIndexProvider = new HDIndexProvider();

        Node node = ((NodeEngineImpl) nodeEngine).getNode();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        if (nodeExtension.isHotRestartEnabled()) {
            this.hotRestartService = (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService();
            this.hotRestartService.registerRamStoreRegistry(MapService.SERVICE_NAME, this);
        }
        ((NodeEngineImpl) nodeEngine).getMetricsRegistry()
                .registerDynamicMetricsProvider(new MetricsProvider(getMapContainers()));
    }

    private QueryRunner createHDMapQueryRunner(HDPartitionScanRunner runner,
                                               QueryOptimizer queryOptimizer,
                                               ResultProcessorRegistry resultProcessorRegistry) {
        return new QueryRunner(this, queryOptimizer,
                new HDPartitionScanExecutor(runner), resultProcessorRegistry);
    }

    @Override
    protected PartitionContainer createPartitionContainer(MapService service, int partitionId) {
        return new EnterprisePartitionContainer(service, partitionId);
    }

    @Override
    protected LocalMapStatsProvider createLocalMapStatsProvider() {
        return new EnterpriseLocalMapStatsProvider(this);
    }

    @Override
    MapContainer createMapContainer(String mapName) {
        return new EnterpriseMapContainer(mapName, getNodeEngine().getConfig(), this);
    }

    @Override
    MapNearCacheManager createMapNearCacheManager() {
        return new EnterpriseMapNearCacheManager(this);
    }

    @Override
    MapEventPublisherImpl createMapEventPublisherSupport() {
        return new EnterpriseMapEventPublisherImpl(this);
    }

    @Override
    public ValueComparator getValueComparatorOf(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            return NativeValueComparator.INSTANCE;
        }
        return super.getValueComparatorOf(inMemoryFormat);
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
        EnterpriseRecordStore recordStore
                = (EnterpriseRecordStore) partitionContainer.getRecordStoreForHotRestart(name);
        return recordStore.getRamStore();
    }

    @Override
    public int prefixToThreadId(long prefix) {
        throw new UnsupportedOperationException("prefixToThreadId() should not have been called on this class");
    }

    @Override
    public MapFilterProvider getMapFilterProvider() {
        return mapFilterProvider;
    }

    @Override
    public QueryRunner getMapQueryRunner(String mapName) {
        if (getInMemoryFormat(mapName) == NATIVE) {
            return hdMapQueryRunner;
        } else {
            return super.getMapQueryRunner(mapName);
        }
    }

    private InMemoryFormat getInMemoryFormat(String mapName) {
        MapContainer container = getMapContainer(mapName);
        MapConfig mapConfig = container.getMapConfig();
        return mapConfig.getInMemoryFormat();
    }

    @Override
    public Data toData(Object object) {
        return ((EnterpriseSerializationService) getNodeEngine().getSerializationService()).toData(object, DataType.HEAP);
    }

    @Override
    protected void removeAllRecordStoresOfAllMaps(boolean onShutdown, boolean onRecordStoreDestroy) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();

        List<EnterpriseMapPartitionClearOperation> operations = new ArrayList<>();
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
                operation.awaitCompletion(MAP_PARTITION_CLEAR_OPERATION_AWAIT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }
    }

    @Override
    public RecordStore createRecordStore(MapContainer mapContainer,
                                         int partitionId, MapKeyLoader keyLoader) {
        NodeEngine nodeEngine = getNodeEngine();
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

            if (!hotRestartService.isStartCompleted()) {
                ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
                UUID source = nodeEngine.getLocalMember().getUuid();
                proxyService.getOrCreateRegistry(MapService.SERVICE_NAME).getOrCreateProxyFuture(name, source, false, false);
            }
        }
        return new EnterpriseRecordStore(mapContainer, partitionId, keyLoader, logger, hotRestartConfig, prefix);
    }

    private HotRestartConfig getHotRestartConfig(MapContainer
                                                         mapContainer) {
        HotRestartConfig hotRestartConfig = mapContainer.getMapConfig().getHotRestartConfig();
        return hotRestartConfig;
    }

    @Override
    public IndexProvider getIndexProvider(MapConfig mapConfig) {
        if (mapConfig.getInMemoryFormat() != NATIVE) {
            return super.getIndexProvider(mapConfig);
        }
        return hdIndexProvider;
    }

    private static final class MetricsProvider implements DynamicMetricsProvider {
        private final Map<String, MapContainer> mapContainers;

        private MetricsProvider(Map<String, MapContainer> mapContainers) {
            this.mapContainers = mapContainers;
        }

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            descriptor.withPrefix(MAP_PREFIX);
            for (MapContainer mapContainer : mapContainers.values()) {
                if (mapContainer instanceof EnterpriseMapContainer) {
                    context.collect(descriptor.copy()
                                              .withDiscriminator(MAP_DISCRIMINATOR_NAME, mapContainer.name),
                            ((EnterpriseMapContainer) mapContainer).getHdStorageInfo());
                }
            }
        }
    }
}
