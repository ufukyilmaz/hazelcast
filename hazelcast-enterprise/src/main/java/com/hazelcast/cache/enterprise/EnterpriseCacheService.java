package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.CacheStorageType;
import com.hazelcast.cache.enterprise.impl.offheap.EnterpriseOffHeapCacheRecordStore;
import com.hazelcast.cache.enterprise.impl.onheap.EnterpriseOnHeapCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheDestroyOperation;
import com.hazelcast.cache.enterprise.operation.CacheReplicationOperation;
import com.hazelcast.cache.enterprise.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.client.CacheInvalidationListener;
import com.hazelcast.cache.client.CacheInvalidationMessage;
import com.hazelcast.cache.impl.*;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 05/02/14
 */
public class EnterpriseCacheService extends CacheService {

    @Override
    protected ConstructorFunction<CacheInfo, ICacheRecordStore> createCacheConstructorFunction(final int partitionId) {
        ConstructorFunction<CacheInfo, ICacheRecordStore> function
                = new ConstructorFunction<CacheInfo, ICacheRecordStore>() {

            public ICacheRecordStore createNew(CacheInfo ci) {
                if (ci.getCacheStorageType() == null
                        || ci.getCacheStorageType() == CacheStorageType.HEAP) {
                    return new EnterpriseOnHeapCacheRecordStore(ci.getName(),
                                                                partitionId,
                                                                nodeEngine,
                                                                EnterpriseCacheService.this);
                } else {
                    try {
                        return new EnterpriseOffHeapCacheRecordStore(partitionId,
                                                                     ci.getName(),
                                                                     EnterpriseCacheService.this,
                                                                     getSerializationService(),
                                                                     nodeEngine,
                                                                     EnterpriseOffHeapCacheRecordStore.DEFAULT_INITIAL_CAPACITY);
                    } catch (OffHeapOutOfMemoryError e) {
                        throw new OffHeapOutOfMemoryError("Cannot create internal cache map, " +
                                "not enough contiguous memory available! -> " + e.getMessage(), e);
                    }
                }
            }
        };
        return function;
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
                EnterpriseCacheRecordStore cache = (EnterpriseCacheRecordStore) getCache(name, partitionId);
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
        CacheReplicationOperation op = new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        /*
        EnterpriseSerializationService serializationService = getSerializationService();
        if (serializationService.getMemoryManager() == null) {
            throw new IllegalStateException("OffHeap memory should be enabled and configured " +
                    "to be able to use ICache!");
        }
        */
        return super.createDistributedObject(objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheDestroyOperation> ops = new ArrayList<CacheDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasCache(objectName)) {
                CacheDestroyOperation op = new CacheDestroyOperation(objectName);
                ops.add(op);
                op.setPartitionId(segment.getPartitionId()).setNodeEngine(nodeEngine).setService(this);
                operationService.executeOperation(op);
            }
        }
        for (CacheDestroyOperation op : ops) {
            try {
                op.awaitCompletion(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String addInvalidationListener(String name, CacheInvalidationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, name, listener);
        return registration.getId();
    }

    public void dispatchEvent(CacheInvalidationMessage event, CacheInvalidationListener listener) {
        listener.send(event);
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

    @Override
    public EnterpriseCacheRecordStore getOrCreateCache(String name, int partitionId) {
        CacheConfig cacheConfig = getCacheConfig(name);
        if (cacheConfig != null && cacheConfig.getCacheStorageType() != null) {
            return (EnterpriseCacheRecordStore) super.getOrCreateCache(name,
                                                                       cacheConfig.getCacheStorageType(),
                                                                       partitionId);
        } else {
            return (EnterpriseCacheRecordStore) super.getOrCreateCache(name, partitionId);
        }
    }

    @Override
    public EnterpriseCacheRecordStore getOrCreateCache(String name,
                                                       CacheStorageType cacheStorageType,
                                                       int partitionId) {
        return (EnterpriseCacheRecordStore) super.getOrCreateCache(name, cacheStorageType, partitionId);
    }

    @Override
    public EnterpriseCacheRecordStore getCache(String name, int partitionId) {
        return (EnterpriseCacheRecordStore) super.getCache(name, partitionId);
    }

    @Override
    public CacheConfig getCacheConfig(String name) {
        CacheConfig cacheConfig = super.getCacheConfig(name);
        if (cacheConfig == null) {
            cacheConfig = new CacheConfig().setName(name);
        }
        return cacheConfig;
    }

    public CacheStatisticsImpl getOrCreateCacheStats(String name) {
        return null;
    }

    @Override
    public String toString() {
        return "EnterpriseCacheService[" + SERVICE_NAME + "]";
    }

}
