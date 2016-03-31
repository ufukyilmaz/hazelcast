package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.event.EnterpriseMapEventPublisherImpl;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.nearcache.EnterpriseNearCacheProvider;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.operation.EnterpriseMapOperationProviders;
import com.hazelcast.map.impl.operation.EnterpriseMapPartitionClearOperation;
import com.hazelcast.map.impl.operation.HDBasePutOperation;
import com.hazelcast.map.impl.operation.HDBaseRemoveOperation;
import com.hazelcast.map.impl.operation.HDGetOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperationProviders;
import com.hazelcast.map.impl.query.HDMapQueryEngineImpl;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.wan.filter.MapFilterProvider;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.hotrestart.HotRestartService;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.PersistentCacheDescriptors;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.query.impl.predicates.QueryOptimizerFactory.newOptimizer;

/**
 * Contains enterprise specific implementations of {@link MapServiceContext}
 * functionality.
 *
 * @see MapServiceContext
 */
class EnterpriseMapServiceContextImpl extends MapServiceContextImpl
        implements EnterpriseMapServiceContext, RamStoreRegistry {

    private static final int MAP_PARTITION_CLEAR_OPERATION_AWAIT_TIME_IN_SECS = 10;

    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            final MapServiceContext mapServiceContext = getService().getMapServiceContext();
            final Config config = mapServiceContext.getNodeEngine().getConfig();
            final MapConfig mapConfig = config.findMapConfig(mapName);
            return new EnterpriseMapContainer(mapName, mapConfig, mapServiceContext);
        }
    };

    private final MapQueryEngine hdMapQueryEngine;
    private final MapFilterProvider mapFilterProvider;

    private HotRestartService hotRestartService;
    private QueryCacheContext queryCacheContext;

    EnterpriseMapServiceContextImpl(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.hdMapQueryEngine = new HDMapQueryEngineImpl(this, newOptimizer(nodeEngine.getGroupProperties()));
        this.mapFilterProvider = new MapFilterProvider(nodeEngine);
        Node node = ((NodeEngineImpl) nodeEngine).getNode();
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        if (nodeExtension.isHotRestartEnabled()) {
            hotRestartService = nodeExtension.getHotRestartService();
            hotRestartService.registerRamStoreRegistry(MapService.SERVICE_NAME, this);
        }
        if (nodeExtension.isFeatureEnabledForLicenseKey(Feature.CONTINUOUS_QUERY_CACHE)) {
            queryCacheContext = new NodeQueryCacheContext(this);
        }
    }

    @Override
    MapOperationProviders createOperationProviders() {
        return new EnterpriseMapOperationProviders(this);
    }

    public HotRestartStore getOnHeapHotRestartStoreForCurrentThread() {
        return hotRestartService.getOnHeapHotRestartStoreForCurrentThread();
    }

    public HotRestartStore getOffHeapHotRestartStoreForCurrentThread() {
        return hotRestartService.getOffHeapHotRestartStoreForCurrentThread();
    }

    @Override
    public RamStore ramStoreForPrefix(long prefix) {
        int partitionId = PersistentCacheDescriptors.toPartitionId(prefix);
        String name = hotRestartService.getCacheName(prefix);
        EnterpriseRecordStore recordStore = (EnterpriseRecordStore) getExistingRecordStore(partitionId, name);
        return recordStore == null ? null : recordStore.getRamStore();
    }

    @Override
    public RamStore restartingRamStoreForPrefix(long prefix) {
        int partitionId = PersistentCacheDescriptors.toPartitionId(prefix);
        String name = hotRestartService.getCacheName(prefix);
        PartitionContainer partitionContainer = getPartitionContainer(partitionId);
        EnterpriseRecordStore recordStore = (EnterpriseRecordStore) partitionContainer.getRecordStoreForHotRestart(name);
        return recordStore.getRamStore();
    }

    @Override
    NearCacheProvider createNearCacheProvider() {
        return new EnterpriseNearCacheProvider(this);
    }

    @Override
    LocalMapStatsProvider createLocalMapStatsProvider() {
        return new EnterpriseLocalMapStatsProvider(this);
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, mapContainers, mapConstructor);
    }

    @Override
    public QueryCacheContext getQueryCacheContext() {
        if (queryCacheContext == null) {
            throw new InvalidLicenseException("Continuous Query Cache is not enabled for your license key."
                    + "Please contact sales@hazelcast.com");
        }
        return queryCacheContext;
    }

    @Override
    public MapFilterProvider getMapFilterProvider() {
        return mapFilterProvider;
    }

    @Override
    public MapQueryEngine getMapQueryEngine(String mapName) {
        InMemoryFormat inMemoryFormat = getInMemoryFormat(mapName);
        if (NATIVE == inMemoryFormat) {
            return hdMapQueryEngine;
        } else {
            return super.getMapQueryEngine(mapName);
        }
    }

    @Override
    public String addListenerAdapter(String cacheName, ListenerAdapter listenerAdaptor) {
        EventService eventService = getNodeEngine().getEventService();
        EventRegistration registration
                = eventService.registerListener(MapService.SERVICE_NAME,
                cacheName, TrueEventFilter.INSTANCE, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addListenerAdapter(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName) {
        EventRegistration registration = getNodeEngine().getEventService().
                registerListener(MapService.SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addLocalListenerAdapter(ListenerAdapter adapter, String mapName) {
        EventService eventService = getNodeEngine().getEventService();
        EventRegistration registration = eventService.registerLocalListener(MapService.SERVICE_NAME, mapName, adapter);
        return registration.getId();
    }

    @Override
    MapEventPublisherImpl createMapEventPublisherSupport() {
        return new EnterpriseMapEventPublisherImpl(this);
    }

    public MapOperationProvider getMapOperationProvider(String name) {
        return operationProviders.getOperationProvider(name);
    }

    @Override
    public Data toData(Object object) {
        return ((EnterpriseSerializationService) getNodeEngine().getSerializationService()).toData(object, DataType.HEAP);
    }


    @Override
    public void clearPartitionData(int partitionId) {
        final PartitionContainer container = partitionContainers[partitionId];
        if (container != null) {
            for (RecordStore recordStore : container.getMaps().values()) {
                recordStore.destroy();
            }
            container.getMaps().clear();
        }
    }

    @Override
    public void clearPartitions(boolean onShutdown) {
        InternalOperationService operationService = (InternalOperationService)nodeEngine.getOperationService();

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
                operationService.runOperationOnCallingThread(operation);
            } else {
                operationService.executeOperation(operation);
                operations.add(operation);
            }
        }

        for (EnterpriseMapPartitionClearOperation operation : operations) {
            try {
                operation.awaitCompletion(MAP_PARTITION_CLEAR_OPERATION_AWAIT_TIME_IN_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }
    }

    @Override
    public void incrementOperationStats(long startTime, LocalMapStatsImpl localMapStats, String mapName, Operation operation) {
        InMemoryFormat inMemoryFormat = getInMemoryFormat(mapName);

        if (NATIVE == inMemoryFormat) {
            if (operation instanceof HDBasePutOperation) {
                localMapStats.incrementPuts(Clock.currentTimeMillis() - startTime);
                return;
            }

            if (operation instanceof HDBaseRemoveOperation) {
                localMapStats.incrementRemoves(Clock.currentTimeMillis() - startTime);
                return;
            }

            if (operation instanceof HDGetOperation) {
                localMapStats.incrementGets(Clock.currentTimeMillis() - startTime);
                return;
            }
        } else {
            super.incrementOperationStats(startTime, localMapStats, mapName, operation);
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
        }
        return new EnterpriseRecordStore(mapContainer, partitionId, keyLoader, logger, hotRestartConfig, prefix);
    }

    private HotRestartConfig getHotRestartConfig(MapContainer mapContainer) {
        final HotRestartConfig hotRestartConfig = mapContainer.getMapConfig().getHotRestartConfig();
        return hotRestartConfig != null ? hotRestartConfig : new HotRestartConfig().setEnabled(false);
    }
}
