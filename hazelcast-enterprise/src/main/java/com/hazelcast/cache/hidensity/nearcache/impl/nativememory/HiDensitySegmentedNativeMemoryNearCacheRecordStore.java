package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Runtime.getRuntime;

/**
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
public class HiDensitySegmentedNativeMemoryNearCacheRecordStore<K, V>
        implements HiDensityNearCacheRecordStore<K, V, HiDensityNativeMemoryNearCacheRecord> {

    private final NearCacheConfig nearCacheConfig;
    private final NearCacheStatsImpl nearCacheStats;
    private final HazelcastMemoryManager memoryManager;

    private final int hashSeed;
    private final int segmentMask;
    private final int segmentShift;

    private HiDensityNativeMemoryNearCacheRecordStore<K, V>[] segments;

    @SuppressWarnings("checkstyle:magicnumber")
    public HiDensitySegmentedNativeMemoryNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                              NearCacheContext nearCacheContext) {
        this.nearCacheConfig = nearCacheConfig;
        this.nearCacheStats = new NearCacheStatsImpl();

        HiDensityStorageInfo storageInfo = new HiDensityStorageInfo(nearCacheConfig.getName());
        int concurrencyLevel = Math.max(16, 8 * getRuntime().availableProcessors());
        // find power-of-two sizes best matching arguments
        int sShift = 0;
        int sSize = 1;
        while (sSize < concurrencyLevel) {
            ++sShift;
            sSize <<= 1;
        }
        this.hashSeed = hashCode();
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

        HazelcastMemoryManager mm = serializationService.getMemoryManager();
        if (mm instanceof PoolingMemoryManager) {
            this.memoryManager = ((PoolingMemoryManager) mm).getGlobalMemoryManager();
        } else {
            this.memoryManager = mm;
        }
    }

    private void checkAvailable() {
        if (segments == null) {
            throw new IllegalStateException(nearCacheConfig.getName() + " named Near Cache record store is not available");
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hash(Object o) {
        int h = hashSeed;

        h ^= o.hashCode();

        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
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

    private HiDensityNativeMemoryNearCacheRecordStore<K, V> segmentFor(K key) {
        int hash = hash(key);
        return segments[(hash >>> segmentShift) & segmentMask];
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
                // So there will be no extra conversion from Object to Data.
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
    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    /**
     * Represents a segment block (lockable by a thread) in this Near Cache storage
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
