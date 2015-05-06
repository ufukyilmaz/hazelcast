package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.EnterprisePostJoinMapOperation;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Extended version of {@link MapPostJoinAwareService} with enterprise only features.
 */
class EnterpriseMapPostJoinAwareService extends MapPostJoinAwareService {

    private EnterpriseMapServiceContext mapServiceContext;

    public EnterpriseMapPostJoinAwareService(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public Operation getPostJoinOperation() {
        EnterprisePostJoinMapOperation operation = new EnterprisePostJoinMapOperation();
        final Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        for (MapContainer mapContainer : mapContainers.values()) {
            operation.addMapIndex(mapContainer);
            operation.addMapInterceptors(mapContainer);
        }
        List<AccumulatorInfo> infoList = getQueryCacheNameAndSequenceList();
        operation.setInfoList(infoList);
        return operation;
    }

    private List<AccumulatorInfo> getQueryCacheNameAndSequenceList() {
        List<AccumulatorInfo> infoList = new ArrayList<AccumulatorInfo>();

        MapPublisherRegistry mapPublisherRegistry = getPublisherContext().getMapPublisherRegistry();
        Map<String, PublisherRegistry> cachesOfMaps = mapPublisherRegistry.getAll();
        Collection<PublisherRegistry> publisherRegistries = cachesOfMaps.values();
        for (PublisherRegistry publisherRegistry : publisherRegistries) {
            Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = publisherRegistry.getAll().values();
            for (PartitionAccumulatorRegistry accumulatorRegistry : partitionAccumulatorRegistries) {
                AccumulatorInfo info = accumulatorRegistry.getInfo();
                infoList.add(info);
            }
        }
        return infoList;
    }

    private PublisherContext getPublisherContext() {
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getPublisherContext();
    }
}
