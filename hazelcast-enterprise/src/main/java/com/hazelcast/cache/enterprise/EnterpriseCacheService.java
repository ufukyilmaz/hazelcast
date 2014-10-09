package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.enterprise.operation.CacheDestroyOperation;
import com.hazelcast.cache.enterprise.operation.CacheReplicationOperation;
import com.hazelcast.cache.enterprise.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.client.CacheInvalidationListener;
import com.hazelcast.cache.client.CacheInvalidationMessage;
import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
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
public class EnterpriseCacheService extends AbstractCacheService implements ICacheService {

    @Override
    protected ConstructorFunction<String, ICacheRecordStore> createCacheConstructorFunction(final int partitionId) {
        ConstructorFunction<String, ICacheRecordStore> function
                = new ConstructorFunction<String, ICacheRecordStore>() {

            public ICacheRecordStore createNew(String name) {
                try {
                    return new EnterpriseCacheRecordStore(name, partitionId, nodeEngine, EnterpriseCacheService.this);
                } catch (OffHeapOutOfMemoryError e) {
                    throw new OffHeapOutOfMemoryError("Cannot create internal cache map, " +
                            "not enough contiguous memory available! -> " + e.getMessage(), e);
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
    int forceEvict(String name, int originalPartitionId) {
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
        EnterpriseSerializationService serializationService = getSerializationService();
        if (serializationService.getMemoryManager() == null) {
            throw new IllegalStateException("OffHeap memory should be enabled and configured " +
                    "to be able to use ICache!");
        }
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
    public String toString() {
        return "CacheService[" + SERVICE_NAME + "]";
    }

}
