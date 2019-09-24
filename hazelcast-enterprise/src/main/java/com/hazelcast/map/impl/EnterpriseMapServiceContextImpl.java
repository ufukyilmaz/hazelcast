package com.hazelcast.map.impl;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
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
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.EnterpriseMetadataRecordStoreMutationObserver;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.map.impl.recordstore.JsonMetadataRecordStoreMutationObserver;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.RecordStoreMutationObserver;
import com.hazelcast.map.impl.wan.MapFilterProvider;
import com.hazelcast.map.impl.wan.MerkleTreeUpdaterRecordStoreMutationObserver;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.HDIndexProvider;
import com.hazelcast.query.impl.IndexProvider;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.PersistentConfigDescriptors;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
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
        Node node = ((NodeEngineImpl) nodeEngine).getNode();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        if (nodeExtension.isHotRestartEnabled()) {
            hotRestartService = (HotRestartIntegrationService) nodeExtension.getInternalHotRestartService();
            hotRestartService.registerRamStoreRegistry(MapService.SERVICE_NAME, this);
        }
        this.hdMapQueryRunner = createHDMapQueryRunner(
                new HDPartitionScanRunner(this), getQueryOptimizer(), getResultProcessorRegistry());
        this.hdIndexProvider = new HDIndexProvider();
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
    ConstructorFunction<String, MapContainer> createMapConstructor() {
        return mapName -> new EnterpriseMapContainer(mapName, getNodeEngine().getConfig(), this);
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
        if (getInMemoryFormat(mapName) != NATIVE) {
            return super.getMapQueryRunner(mapName);
        }
        return hdMapQueryRunner;
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

            ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
            proxyService.getOrCreateRegistry(MapService.SERVICE_NAME).getOrCreateProxyFuture(name, false, false);
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

    @Override
    public Collection<RecordStoreMutationObserver<Record>> createRecordStoreMutationObservers(String mapName, int partitionId) {
        Collection<RecordStoreMutationObserver<Record>> observers = new LinkedList<>();
        addMerkleTreeUpdaterObserver(observers, mapName, partitionId);

        observers.addAll(super.createRecordStoreMutationObservers(mapName, partitionId));

        return observers;
    }

    @Override
    protected void addMetadataInitializerObserver(Collection<RecordStoreMutationObserver<Record>> observers,
                                                  String mapName, int partitionId) {
        MapContainer mapContainer = getMapContainer(mapName);
        MetadataPolicy policy = mapContainer.getMapConfig().getMetadataPolicy();
        if (policy == MetadataPolicy.CREATE_ON_UPDATE) {
            RecordStoreMutationObserver<Record> observer;
            if (mapContainer.getMapConfig().getInMemoryFormat() == NATIVE) {
                observer = new EnterpriseMetadataRecordStoreMutationObserver(getSerializationService(),
                        JsonMetadataInitializer.INSTANCE, this, mapName, partitionId);
            } else {
                observer = new JsonMetadataRecordStoreMutationObserver(getSerializationService(),
                        JsonMetadataInitializer.INSTANCE);
            }
            observers.add(observer);
        }
    }

    /**
     * Adds a merkle tree record store observer to {@code observers} if merkle
     * trees are configured for the map with the {@code mapName} name.
     *
     * @param observers   the collection of record store observers
     * @param mapName     the map name
     * @param partitionId The partition ID
     */
    private void addMerkleTreeUpdaterObserver(Collection<RecordStoreMutationObserver<Record>> observers,
                                              String mapName, int partitionId) {
        EnterprisePartitionContainer partitionContainer
                = (EnterprisePartitionContainer) getPartitionContainer(partitionId);
        MerkleTree merkleTree = partitionContainer.getOrCreateMerkleTree(mapName);
        if (merkleTree != null) {
            observers.add(new MerkleTreeUpdaterRecordStoreMutationObserver<>(merkleTree,
                    getNodeEngine().getSerializationService()));
        }
    }
}
