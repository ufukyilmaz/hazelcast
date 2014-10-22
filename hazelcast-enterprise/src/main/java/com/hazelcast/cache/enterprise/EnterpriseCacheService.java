package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.CacheOperationProvider;
import com.hazelcast.cache.OffHeapOperationProvider;
import com.hazelcast.cache.client.CacheInvalidationListener;
import com.hazelcast.cache.client.CacheInvalidationMessage;
import com.hazelcast.cache.enterprise.impl.hidensity.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.enterprise.operation.EnterpriseCacheReplicationOperation;
import com.hazelcast.cache.impl.*;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 05/02/14
 */
public class EnterpriseCacheService extends CacheService {

    @Override
    protected ICacheRecordStore createNewRecordStore(String name, int partitionId) {
        CacheConfig cacheConfig = configs.get(name);
        if (cacheConfig == null) {
            throw new IllegalArgumentException("CacheConfig is null!!! " + name);
        }
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        if (InMemoryFormat.OFFHEAP.equals(inMemoryFormat)) {
            try {
                return new HiDensityNativeMemoryCacheRecordStore(partitionId,
                        name,
                        this,
                        nodeEngine,
                        HiDensityNativeMemoryCacheRecordStore.DEFAULT_INITIAL_CAPACITY);
            } catch (OffHeapOutOfMemoryError e) {
                throw new OffHeapOutOfMemoryError("Cannot create internal cache map, " +
                        "not enough contiguous memory available! -> " + e.getMessage(), e);
            }

        } else if (InMemoryFormat.BINARY.equals(inMemoryFormat)
                || InMemoryFormat.OBJECT.equals(inMemoryFormat)) {
            return super.createNewRecordStore(name, partitionId);
        }
        // TODO:
        //      Or if "inMemoryFormat" is "ON_HEAP_SLAB",
        //      create "EnterpriseOnHeapSlabAllocatorCacheRecordStore"
        throw new IllegalArgumentException("Cannot create record store for the storage type: " + inMemoryFormat);
    }

    @Override
    public void reset() {
        shutdown(false);
    }

    @Override
    public void shutdown(boolean terminate) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheSegmentDestroyOperation> ops = new ArrayList<CacheSegmentDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasAnyCache()) {
                CacheSegmentDestroyOperation op = new CacheSegmentDestroyOperation();
                ops.add(op);
                op.setPartitionId(segment.getPartitionId()).setNodeEngine(nodeEngine).setService(this);
                operationService.executeOperation(op);
            }
        }
        for (CacheSegmentDestroyOperation op : ops) {
            try {
                op.awaitCompletion(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Does forced eviction on one or more caches... Runs on the operation threads..
     */
    public int forceEvict(String name, int originalPartitionId) {
        int evicted = 0;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = nodeEngine.getOperationService().getPartitionOperationThreadCount();
        int mod = originalPartitionId % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ICacheRecordStore cache = getCacheRecordStore(name, partitionId);
                if (cache != null) {
                    evicted += cache.forceEvict();
                }
            }
        }
        return evicted;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        EnterpriseCacheReplicationOperation op = new EnterpriseCacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    public String addInvalidationListener(String name, CacheInvalidationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, name, listener);
        return registration.getId();
    }

    public void sendInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (!registrations.isEmpty()) {
            EnterpriseSerializationService ss = getSerializationService();
            Data event = ss.convertData(key, DataType.HEAP);
            eventService.publishEvent(SERVICE_NAME, registrations,
                    new CacheInvalidationMessage(name, event, sourceUuid), name.hashCode());
        }
    }

    public EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) nodeEngine.getSerializationService();
    }

    public CacheStatisticsImpl getOrCreateCacheStats(String name) {
        return null;
    }

    @Override
    public CacheOperationProvider getCacheOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.OFFHEAP.equals(inMemoryFormat)) {
            return new OffHeapOperationProvider(nameWithPrefix);
        }
        return super.getCacheOperationProvider(nameWithPrefix, inMemoryFormat);
    }

    @Override
    public String toString() {
        return "EnterpriseCacheService[" + SERVICE_NAME + "]";
    }

}
