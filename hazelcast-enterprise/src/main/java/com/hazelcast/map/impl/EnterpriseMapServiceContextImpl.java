package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.event.EnterpriseMapEventPublisherImpl;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.operation.HDBasePutOperation;
import com.hazelcast.map.impl.operation.HDBaseRemoveOperation;
import com.hazelcast.map.impl.operation.HDGetOperation;
import com.hazelcast.map.impl.operation.HDMapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.query.HDMapQueryEngineImpl;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
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
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.query.impl.predicates.QueryOptimizerFactory.newOptimizer;

/**
 * Contains enterprise specific implementations of {@link MapServiceContext}
 * functionality.
 *
 * @see MapServiceContext
 */
class EnterpriseMapServiceContextImpl extends MapServiceContextImpl implements EnterpriseMapServiceContext {

    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            final MapServiceContext mapServiceContext = getService().getMapServiceContext();
            final Config config = mapServiceContext.getNodeEngine().getConfig();
            final MapConfig mapConfig = config.findMapConfig(mapName);
            final EnterpriseMapContainer mapContainer
                    = new EnterpriseMapContainer(mapName, mapConfig, mapServiceContext);
            return mapContainer;
        }
    };

    private final QueryCacheContext queryCacheContext;
    private final MapQueryEngine hdMapQueryEngine;
    private final MapOperationProvider hdMapOperationProvider;
    private final MapFilterProvider mapFilterProvider;

    EnterpriseMapServiceContextImpl(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.queryCacheContext = new NodeQueryCacheContext(this);
        this.hdMapQueryEngine = new HDMapQueryEngineImpl(this,
                newOptimizer(nodeEngine.getGroupProperties()));
        this.hdMapOperationProvider = new HDMapOperationProvider();
        this.mapFilterProvider = new MapFilterProvider(nodeEngine);
    }

    @Override
    LocalMapStatsProvider createLocalMapStatsProvider() {
        return new EnterpriseLocalMapStatsProvider(this);
    }

    @Override
    PartitionContainer[] createPartitionContainers() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return new EnterprisePartitionContainer[partitionCount];
    }

    @Override
    public void initPartitionsContainers() {
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final PartitionContainer[] partitionContainers = this.partitionContainers;
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new EnterprisePartitionContainer(getService(), i);
        }
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, mapContainers, mapConstructor);
    }

    @Override
    public QueryCacheContext getQueryCacheContext() {
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
        InMemoryFormat inMemoryFormat = getInMemoryFormat(name);

        if (NATIVE == inMemoryFormat) {
            return hdMapOperationProvider;
        }
        return super.getMapOperationProvider(name);
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
                recordStore.clearPartition();
                recordStore.destroy();
            }
            container.getMaps().clear();
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
}
