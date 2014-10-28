/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.CacheDestroyOperation;
import com.hazelcast.cache.CacheOperationProvider;
import com.hazelcast.cache.HiDensityOperationProvider;
import com.hazelcast.cache.client.CacheInvalidationListener;
import com.hazelcast.cache.client.CacheInvalidationMessage;
import com.hazelcast.cache.enterprise.impl.hidensity.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.enterprise.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.enterprise.operation.HiDensityCacheReplicationOperation;
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
import java.util.concurrent.ConcurrentMap;
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
        throw new IllegalArgumentException("Cannot create record store for the storage type: "
                + inMemoryFormat);
    }

    @Override
    public void reset() {
        shutdown(false);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        destroySegments(objectName);
    }

    @Override
    protected void destroySegments(String objectName) {
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheDestroyOperation> ops = new ArrayList<CacheDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasCache(objectName)) {
                CacheDestroyOperation op = new CacheDestroyOperation(objectName);
                ops.add(op);
                op.setPartitionId(segment.getPartitionId())
                        .setNodeEngine(nodeEngine).setService(this);
                if (op.getPartitionId() == 0) {
                    operationService.executeOperation(op);
                } else {
                    operationService.executeOperation(op);
                }
            }
        }
        // TODO: This is commented-out since
        // there is a deadlock between HiDensity cache destroy and open-source destroy operations
        // Currently operations are fire and forget :)
        /*
        for (CacheDestroyOperation op : ops) {
            try {
                op.awaitCompletion(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        */
    }

    @Override
    public void shutdown(boolean terminate) {
        final ConcurrentMap<String, CacheConfig> cacheConfigs = configs;
        for (String objectName : cacheConfigs.keySet()) {
            destroyCache(objectName, true, null);
        }
        OperationService operationService = nodeEngine.getOperationService();
        List<CacheSegmentDestroyOperation> ops = new ArrayList<CacheSegmentDestroyOperation>();
        for (CachePartitionSegment segment : segments) {
            if (segment.hasAnyCache()) {
                CacheSegmentDestroyOperation op = new CacheSegmentDestroyOperation();
                ops.add(op);
                op.setPartitionId(segment.getPartitionId())
                        .setNodeEngine(nodeEngine).setService(this);
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
        HiDensityCacheReplicationOperation op =
                new HiDensityCacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    public String addInvalidationListener(String name, CacheInvalidationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration =
                eventService.registerLocalListener(SERVICE_NAME, name, listener);
        return registration.getId();
    }

    public void sendInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations =
                eventService.getRegistrations(SERVICE_NAME, name);
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

    public CacheStatisticsImpl getOrCreateCacheStats(String nameWithPrefix) {
        return createCacheStatIfAbsent(nameWithPrefix);
    }

    @Override
    public CacheOperationProvider getCacheOperationProvider(String nameWithPrefix,
                                                            InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.OFFHEAP.equals(inMemoryFormat)) {
            return new HiDensityOperationProvider(nameWithPrefix);
        }

        return super.getCacheOperationProvider(nameWithPrefix, inMemoryFormat);
    }

    @Override
    public String toString() {
        return "EnterpriseCacheService[" + SERVICE_NAME + "]";
    }

}
