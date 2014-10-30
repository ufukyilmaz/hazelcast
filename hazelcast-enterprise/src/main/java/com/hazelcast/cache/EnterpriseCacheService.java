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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.hidensity.operation.CacheDestroyOperation;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheReplicationOperation;
import com.hazelcast.cache.hidensity.operation.HiDensityOperationProvider;
import com.hazelcast.cache.hidensity.client.CacheInvalidationListener;
import com.hazelcast.cache.hidensity.client.CacheInvalidationMessage;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.operation.CacheSegmentDestroyOperation;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
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
 * The {@link CacheService} implementation specified for enterprise usage.
 * This {@link EnterpriseCacheService} implementation mainly handles
 * <ul>
 * <li>
 * {@link ICacheRecordStore} creation of caches with specified partition id
 * </li>
 * <li>
 * Destroying segments and caches
 * </li>
 * <li>
 * Mediating for cache events and listeners
 * </li>
 * </ul>
 *
 * @author mdogan 05/02/14
 */
public class EnterpriseCacheService extends CacheService {

    private static final int CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS = 30;

    /**
     * Creates new {@link ICacheRecordStore} as specified {@link InMemoryFormat}.
     *
     * @param name        the name of the cache with prefix
     * @param partitionId the partition id which cache record store is created on
     * @return the created {@link ICacheRecordStore}
     *
     * @see com.hazelcast.cache.impl.CacheRecordStore
     * @see com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore
     */
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
                throw new OffHeapOutOfMemoryError("Cannot create internal cache map, "
                        + "not enough contiguous memory available! -> " + e.getMessage(), e);
            }
        } else if (inMemoryFormat == null
                || InMemoryFormat.BINARY.equals(inMemoryFormat)
                || InMemoryFormat.OBJECT.equals(inMemoryFormat)) {
            return super.createNewRecordStore(name, partitionId);
        }

        throw new IllegalArgumentException("Cannot create record store for the storage type: "
                + inMemoryFormat);
    }

    /**
     * Destroys the segments for specified <code>object name/cache name</code>.
     *
     * @param objectName the name of object/cache whose segments will be destroyed
     */
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
                operationService.executeOperation(op);
            }
        }
        // TODO This is commented-out since
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

    /**
     * Destroys the distributed object for specified <code>object name/cache name</code>.
     *
     * @param objectName the name of object/cache to be destroyed
     */
    @Override
    public void destroyDistributedObject(String objectName) {
        destroySegments(objectName);
    }

    /**
     * Shutdowns the cache service and destroy the caches with their segments.
     *
     * @param terminate condition about cache service will be closed or not
     */
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
                op.awaitCompletion(CACHE_SEGMENT_DESTROY_OPERATION_AWAIT_TIME_IN_SECS,
                        TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Resets the cache service without closing.
     */
    @Override
    public void reset() {
        shutdown(false);
    }

    /**
     * Does forced eviction on one or more caches. Runs on the operation threads.
     *
     * @param name                the name of the cache to be evicted
     * @param originalPartitionId the partition id of the record store stores the records of cache
     * @return the number of evicted records
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

    /**
     * Creates a {@link HiDensityCacheReplicationOperation} to start the replication.
     *
     * @param event the {@link PartitionReplicationEvent} holds the <code>partitionId</code>
     *              and <code>replica index</code>
     * @return the created {@link HiDensityCacheReplicationOperation}
     */
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        HiDensityCacheReplicationOperation op =
                new HiDensityCacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    /**
     * Registers and {@link CacheInvalidationListener} for specified <code>cacheName</code>.
     *
     * @param cacheName the name of the cache that {@link CacheInvalidationListener} will be registered for
     * @param listener  the {@link CacheInvalidationListener} to be registered for specified <code>cache</code>
     * @return the id which is unique for current registration
     */
    public String addInvalidationListener(String cacheName, CacheInvalidationListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration =
                eventService.registerLocalListener(SERVICE_NAME, cacheName, listener);
        return registration.getId();
    }

    /**
     * Sends an invalidation event for given <code>cacheName</code> with specified <code>key</code>
     * from mentioned source with <code>sourceUuid</code>.
     *
     * @param cacheName  the name of the cache that invalidation event is sent for
     * @param key        the {@link Data} represents the invalidation event
     * @param sourceUuid an id that represents the source for invalidation event
     */
    public void sendInvalidationEvent(String cacheName, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations =
                eventService.getRegistrations(SERVICE_NAME, cacheName);
        if (!registrations.isEmpty()) {
            EnterpriseSerializationService ss = getSerializationService();
            Data event = ss.convertData(key, DataType.HEAP);
            eventService.publishEvent(SERVICE_NAME, registrations,
                    new CacheInvalidationMessage(cacheName, event, sourceUuid), cacheName.hashCode());
        }
    }

    /**
     * Creates a {@link CacheOperationProvider} as specified {@link InMemoryFormat}
     * for specified <code>cacheNameWithPrefix</code>
     *
     * @param cacheNameWithPrefix the name of the cache with prefix that operation works on
     * @param inMemoryFormat      the format of memory such as <code>BINARY</code>, <code>OBJECT</code>
     *                            or <code>OFFHEAP</code>
     * @return
     */
    @Override
    public CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix,
                                                            InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.OFFHEAP.equals(inMemoryFormat)) {
            return new HiDensityOperationProvider(cacheNameWithPrefix);
        }
        return super.getCacheOperationProvider(cacheNameWithPrefix, inMemoryFormat);
    }

    /**
     * Gets the {@link EnterpriseSerializationService} used by this {@link CacheService}.
     *
     * @return the used {@link EnterpriseSerializationService}
     */
    public EnterpriseSerializationService getSerializationService() {
        return (EnterpriseSerializationService) nodeEngine.getSerializationService();
    }

    @Override
    public String toString() {
        return "EnterpriseCacheService[" + SERVICE_NAME + "]";
    }

}
