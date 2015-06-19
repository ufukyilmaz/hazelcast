package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author sozal 24/02/15
 *
 * @param <K> the type of the key stored in near-cache
 * @param <V> the type of the value stored in near-cache
 */
public class HiDensitySegmentedNativeMemoryNearCacheRecordStore<K, V>
        implements HiDensityNearCacheRecordStore<K, V, HiDensityNativeMemoryNearCacheRecord> {

    private final NearCacheConfig nearCacheConfig;
    private final NearCacheStatsImpl nearCacheStats;
    private final HiDensityStorageInfo storageInfo;
    private final MemoryManager memoryManager;

    private HiDensityNativeMemoryNearCacheRecordStore<K, V>[] segments;
    private final int hashSeed;
    private final int segmentMask;
    private final int segmentShift;

    //CHECKSTYLE:OFF
    public HiDensitySegmentedNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                              NearCacheContext nearCacheContext) {
        this.nearCacheConfig = nearCacheConfig;
        this.nearCacheStats = new NearCacheStatsImpl();
        this.storageInfo = new HiDensityStorageInfo(nearCacheConfig.getName());

        int cores = Runtime.getRuntime().availableProcessors() * 8;
        int concurrencyLevel = Math.max(cores, 16);
        // Find power-of-two sizes best matching arguments
        int sShift = 0;
        int sSize = 1;
        while (sSize < concurrencyLevel) {
            ++sShift;
            sSize <<= 1;
        }
        this.hashSeed = this.hashCode();
        this.segmentShift = 32 - sShift;
        this.segmentMask = sSize - 1;
        this.segments = new HiDensityNativeMemoryNearCacheRecordStore[sSize];
        for (int i = 0; i < sSize; i++) {
            this.segments[i] =
                    new HiDensityNativeMemoryNearCacheRecordStoreSegment(
                            nearCacheConfig,
                            nearCacheContext,
                            nearCacheStats,
                            storageInfo);
        }
        EnterpriseSerializationService serializationService =
                (EnterpriseSerializationService) nearCacheContext.getSerializationService();

        MemoryManager mm = serializationService.getMemoryManager();
        if (mm instanceof PoolingMemoryManager) {
            this.memoryManager = ((PoolingMemoryManager) mm).getGlobalMemoryManager();
        } else {
            this.memoryManager = mm;
        }
    }
    //CHECKSTYLE:ON

    private void checkAvailable() {
        if (segments == null) {
            throw new IllegalStateException(nearCacheConfig.getName()
                    + " named near cache record store is not available");
        }
    }

    //CHECKSTYLE:OFF
    private int hash(Object o) {
        int h = hashSeed;

        h ^= o.hashCode();

        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);

        return h ^ (h >>> 16);
    }
    //CHECKSTYLE:ON

    private HiDensityNativeMemoryNearCacheRecordStore segmentFor(K key) {
        int hash = hash(key);
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    @Override
    public V get(K key) {
        checkAvailable();

        HiDensityNativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.get(key);
    }

    @Override
    public void put(K key, V value) {
        checkAvailable();

        HiDensityNativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        segment.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        checkAvailable();

        HiDensityNativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.remove(key);
    }

    @Override
    public void clear() {
        checkAvailable();

        for (HiDensityNativeMemoryNearCacheRecordStore segment : segments) {
            segment.clear();
        }

        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
    }

    @Override
    public void destroy() {
        checkAvailable();

        for (HiDensityNativeMemoryNearCacheRecordStore segment : segments) {
            segment.destroy();
        }

        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);

        segments = null;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        checkAvailable();

        return nearCacheStats;
    }

    @Override
    public Object selectToSave(Object... candidates) {
        Object selectedCandidate = null;
        if (candidates != null && candidates.length > 0) {
            for (Object candidate : candidates) {
                // Give priority to Data typed candidate.
                // So there will be no extra convertion from Object to Data.
                if (candidate instanceof Data) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                // Select a non-null candidate
                for (Object candidate : candidates) {
                    if (candidate != null) {
                        selectedCandidate = candidate;
                        break;
                    }
                }
            }
        }
        return selectedCandidate;
    }

    @Override
    public int size() {
        checkAvailable();

        int size = 0;
        for (HiDensityNativeMemoryNearCacheRecordStore segment : segments) {
            size += segment.size();
        }
        return size;
    }

    @Override
    public int forceEvict() {
        checkAvailable();

        int evictedCount = 0;
        for (int i = 0; i < segments.length; i++) {
            HiDensityNativeMemoryNearCacheRecordStore segment = segments[i];
            evictedCount += segment.forceEvict();
        }
        return evictedCount;
    }

    @Override
    public void doExpiration() {
        checkAvailable();

        Thread currentThread = Thread.currentThread();
        for (int i = 0; i < segments.length; i++) {
            if (currentThread.isInterrupted()) {
                return;
            }
            HiDensityNativeMemoryNearCacheRecordStore segment = segments[i];
            segment.doExpiration();
        }
    }

    @Override
    public void doEvictionIfRequired() {
        checkAvailable();

        Thread currentThread = Thread.currentThread();
        for (int i = 0; i < segments.length; i++) {
            if (currentThread.isInterrupted()) {
                return;
            }
            HiDensityNativeMemoryNearCacheRecordStore segment = segments[i];
            segment.doEvictionIfRequired();
        }
    }

    @Override
    public void doEviction() {
        checkAvailable();

        Thread currentThread = Thread.currentThread();
        for (int i = 0; i < segments.length; i++) {
            if (currentThread.isInterrupted()) {
                return;
            }
            HiDensityNativeMemoryNearCacheRecordStore segment = segments[i];
            segment.doEviction();
        }
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    /**
     * Represents a segment block (lockable by a thread) in this near-cache storage
     */
    private class HiDensityNativeMemoryNearCacheRecordStoreSegment
            extends HiDensityNativeMemoryNearCacheRecordStore<K, V> {

        private static final long READ_LOCK_TIMEOUT_IN_MILLISECONDS = 25;

        private final Lock lock = new ReentrantLock();

        public HiDensityNativeMemoryNearCacheRecordStoreSegment(NearCacheConfig nearCacheConfig,
                                                                NearCacheContext nearCacheContext,
                                                                NearCacheStatsImpl nearCacheStats,
                                                                HiDensityStorageInfo storageInfo) {
            super(nearCacheConfig, nearCacheContext, nearCacheStats, storageInfo);
        }

        @Override
        public V get(K key) {
            try {
                if (lock.tryLock(READ_LOCK_TIMEOUT_IN_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                    try {
                        return super.get(key);
                    } finally {
                        lock.unlock();
                    }
                }
                return null;
            } catch (InterruptedException e) {
                return null;
            }
        }

        @Override
        public void put(K key, V value) {
            lock.lock();
            try {
                super.put(key, value);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean remove(K key) {
            lock.lock();
            try {
                return super.remove(key);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void clear() {
            lock.lock();
            try {
                super.clear();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void destroy() {
            lock.lock();
            try {
                super.destroy();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int forceEvict() {
            lock.lock();
            try {
                return super.forceEvict();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void doExpiration() {
            lock.lock();
            try {
                super.doExpiration();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void doEvictionIfRequired() {
            lock.lock();
            try {
                super.doEvictionIfRequired();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void doEviction() {
            lock.lock();
            try {
                super.doEviction();
            } finally {
                lock.unlock();
            }
        }

    }

}
