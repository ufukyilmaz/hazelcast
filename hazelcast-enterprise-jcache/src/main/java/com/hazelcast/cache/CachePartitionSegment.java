package com.hazelcast.cache;

import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 05/02/14
 */
final class CachePartitionSegment {

    private final NodeEngine nodeEngine;
    private final CacheService cacheService;
    private final int partitionId;
    private final ConcurrentMap<String, CacheRecordStore> caches
            = new ConcurrentHashMap<String, CacheRecordStore>(16, .91f, 1);
    private final Object mutex = new Object();

    private final ConstructorFunction<String, CacheRecordStore> cacheConstructorFunction =
            new ConstructorFunction<String, CacheRecordStore>() {
                public CacheRecordStore createNew(String name) {
                    return newRecordStore(name);
                }
            };

    CachePartitionSegment(NodeEngine nodeEngine, CacheService cacheService, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    int getPartitionId() {
        return partitionId;
    }

    CacheRecordStore getOrCreateCache(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(caches, name, mutex, cacheConstructorFunction);
    }

    private CacheRecordStore newRecordStore(String name) {
        try {
            return new CacheRecordStore(name, partitionId, nodeEngine, cacheService);
        } catch (OffHeapOutOfMemoryError e) {
            throw new OffHeapOutOfMemoryError("Cannot create internal cache map, " +
                    "not enough contiguous memory available! -> " + e.getMessage(), e);
        }
    }

    CacheRecordStore getCache(String name) {
        return caches.get(name);
    }

    Iterator<CacheRecordStore> cacheIterator() {
        return caches.values().iterator();
    }

    void deleteCache(String name) {
        CacheRecordStore cache = caches.remove(name);
        if (cache != null) {
            cache.destroy();
        }
    }

    void clear() {
        synchronized (mutex) {
            for (CacheRecordStore cache : caches.values()) {
                cache.destroy();
            }
        }
        caches.clear();
    }

    void destroy() {
        clear();
    }

    boolean hasAnyCache() {
        return !caches.isEmpty();
    }

    boolean hasCache(String name) {
        return caches.containsKey(name);
    }
}
