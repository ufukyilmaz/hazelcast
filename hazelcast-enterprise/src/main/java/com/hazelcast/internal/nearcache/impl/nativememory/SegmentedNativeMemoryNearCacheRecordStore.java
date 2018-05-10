package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.nearcache.HiDensityNearCacheRecordStore;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.nearcache.impl.preloader.NearCachePreloader;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector.ALWAYS_FRESH;
import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.Thread.currentThread;

/**
 * Segmented {@link HiDensityNearCacheRecordStore} which improves performance by using multiple
 * {@link NativeMemoryNearCacheRecordStore} instances in parallel.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
public class SegmentedNativeMemoryNearCacheRecordStore<K, V>
        implements HiDensityNearCacheRecordStore<K, V, NativeMemoryNearCacheRecord> {

    private final int hashSeed;
    private final int segmentMask;
    private final int segmentShift;
    private final EnterpriseSerializationService serializationService;
    private final ClassLoader classLoader;
    private final NearCacheStatsImpl nearCacheStats;
    private final HazelcastMemoryManager memoryManager;
    private final NativeMemoryNearCacheRecordStore<K, V>[] segments;
    private final NearCachePreloader<Data> nearCachePreloader;
    private final boolean evictionDisabled;

    private volatile StaleReadDetector staleReadDetector = ALWAYS_FRESH;

    @SuppressWarnings("checkstyle:magicnumber")
    public SegmentedNativeMemoryNearCacheRecordStore(String name,
                                                     NearCacheConfig nearCacheConfig,
                                                     EnterpriseSerializationService serializationService,
                                                     ClassLoader classLoader) {
        this.serializationService = serializationService;
        this.classLoader = classLoader;
        this.nearCacheStats = new NearCacheStatsImpl();
        this.memoryManager = getMemoryManager(serializationService);
        this.evictionDisabled = nearCacheConfig.getEvictionConfig().getEvictionPolicy() == NONE;
        int concurrencyLevel = Math.max(16, 8 * RuntimeAvailableProcessors.get());
        // find power-of-two sizes best matching arguments
        int segmentShift = 0;
        int segmentSize = 1;
        while (segmentSize < concurrencyLevel) {
            ++segmentShift;
            segmentSize <<= 1;
        }
        this.hashSeed = hashCode();
        this.segmentMask = segmentSize - 1;
        this.segmentShift = 32 - segmentShift;

        HiDensityStorageInfo storageInfo = new HiDensityStorageInfo(nearCacheConfig.getName());
        this.segments = createSegments(nearCacheConfig, nearCacheStats, storageInfo, segmentSize);

        this.nearCachePreloader = createPreloader(name, nearCacheConfig, serializationService);
    }

    @Override
    public void initialize() {
    }

    private HazelcastMemoryManager getMemoryManager(EnterpriseSerializationService serializationService) {
        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            return ((PoolingMemoryManager) memoryManager).getGlobalMemoryManager();
        }
        return memoryManager;
    }

    @SuppressWarnings("unchecked")
    private NativeMemoryNearCacheRecordStore<K, V>[] createSegments(NearCacheConfig nearCacheConfig,
                                                                    NearCacheStatsImpl nearCacheStats,
                                                                    HiDensityStorageInfo storageInfo,
                                                                    int segmentSize) {
        NativeMemoryNearCacheRecordStore<K, V>[] segments = new NativeMemoryNearCacheRecordStore[segmentSize];
        for (int i = 0; i < segmentSize; i++) {
            segments[i] = new HiDensityNativeMemoryNearCacheRecordStoreSegment(nearCacheConfig, nearCacheStats,
                    storageInfo, serializationService, classLoader);
            segments[i].initialize();
        }
        return segments;
    }

    private NearCachePreloader<Data> createPreloader(String name, NearCacheConfig nearCacheConfig,
                                                     SerializationService serializationService) {
        NearCachePreloaderConfig preloaderConfig = nearCacheConfig.getPreloaderConfig();
        if (preloaderConfig.isEnabled()) {
            return new NearCachePreloader<Data>(name, preloaderConfig, nearCacheStats, serializationService);
        }
        return null;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hash(Object o) {
        int h = hashSeed;

        h ^= o.hashCode();

        // spread bits to regularize both segment and index locations, using variant of single-word Wang/Jenkins hash
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
    }

    @Override
    public V get(K key) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.get(key);
    }

    @Override
    public NearCacheRecord getRecord(K key) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.getRecord(key);
    }

    @Override
    public void put(K key, Data keyData, V value) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        segment.put(key, keyData, value);
    }

    @Override
    public boolean remove(K key) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.remove(key);
    }

    @Override
    public boolean invalidate(K key) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.invalidate(key);
    }

    private NativeMemoryNearCacheRecordStore<K, V> segmentFor(K key) {
        int hash = hash(key);
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    @Override
    public void clear() {
        for (NativeMemoryNearCacheRecordStore segment : segments) {
            segment.clear();
        }

        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);
    }

    @Override
    public void destroy() {
        for (NativeMemoryNearCacheRecordStore segment : segments) {
            segment.destroy();
        }

        nearCacheStats.setOwnedEntryCount(0);
        nearCacheStats.setOwnedEntryMemoryCost(0L);

        if (nearCachePreloader != null) {
            nearCachePreloader.destroy();
        }
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheStats;
    }

    @Override
    public Object selectToSave(Object... candidates) {
        Object selectedCandidate = null;
        if (candidates != null && candidates.length > 0) {
            for (Object candidate : candidates) {
                // give priority to Data typed candidate, so there will be no extra conversion from Object to Data
                if (candidate instanceof Data) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                // select a non-null candidate
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
        int size = 0;
        for (NativeMemoryNearCacheRecordStore segment : segments) {
            size += segment.size();
        }
        return size;
    }

    @Override
    public int forceEvict() {
        int evictedCount = 0;
        for (int i = 0; i < segments.length; i++) {
            NativeMemoryNearCacheRecordStore segment = segments[i];
            evictedCount += segment.forceEvict();
        }
        return evictedCount;
    }

    @Override
    public void doExpiration() {
        Thread currentThread = Thread.currentThread();
        for (int i = 0; i < segments.length; i++) {
            if (currentThread.isInterrupted()) {
                return;
            }
            NativeMemoryNearCacheRecordStore segment = segments[i];
            segment.doExpiration();
        }
    }

    @Override
    public void doEvictionIfRequired() {
        if (evictionDisabled) {
            // if eviction disabled, we are going to short-circuit the currently extremely expensive eviction
            return;
        }

        Thread currentThread = Thread.currentThread();
        for (int i = 0; i < segments.length; i++) {
            if (currentThread.isInterrupted()) {
                return;
            }
            NativeMemoryNearCacheRecordStore segment = segments[i];
            segment.doEvictionIfRequired();
        }
    }

    @Override
    public void doEviction() {
        if (evictionDisabled) {
            // if eviction disabled, we are going to short-circuit the currently extremely expensive eviction
            return;
        }

        Thread currentThread = Thread.currentThread();
        for (int i = 0; i < segments.length; i++) {
            if (currentThread.isInterrupted()) {
                return;
            }
            NativeMemoryNearCacheRecordStore segment = segments[i];
            segment.doEviction();
        }
    }

    @Override
    public void loadKeys(DataStructureAdapter<Object, ?> adapter) {
        nearCachePreloader.loadKeys(adapter);
    }

    @Override
    public void storeKeys() {
        LockableNearCacheRecordStoreSegmentIterator iterator = new LockableNearCacheRecordStoreSegmentIterator(segments);
        try {
            nearCachePreloader.storeKeys(iterator);
        } finally {
            closeResource(iterator);
        }
    }

    @Override
    public void setStaleReadDetector(StaleReadDetector staleReadDetector) {
        this.staleReadDetector = staleReadDetector;
        for (NativeMemoryNearCacheRecordStore<K, V> segment : segments) {
            segment.setStaleReadDetector(staleReadDetector);
        }
    }

    @Override
    public StaleReadDetector getStaleReadDetector() {
        return staleReadDetector;
    }

    @Override
    public long tryReserveForUpdate(K key, Data keyData) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.tryReserveForUpdate(key, keyData);
    }

    @Override
    public V tryPublishReserved(K key, V value, long reservationId, boolean deserialize) {
        NativeMemoryNearCacheRecordStore<K, V> segment = segmentFor(key);
        return segment.tryPublishReserved(key, value, reservationId, deserialize);
    }

    @Override
    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    /**
     * Represents a segment block (lockable by a thread) in this Near Cache storage.
     */
    class HiDensityNativeMemoryNearCacheRecordStoreSegment
            extends NativeMemoryNearCacheRecordStore<K, V>
            implements LockableNearCacheRecordStoreSegment {

        private static final long READ_LOCK_TIMEOUT_IN_MILLISECONDS = 25;

        private final Lock lock = new ReentrantLock();

        HiDensityNativeMemoryNearCacheRecordStoreSegment(NearCacheConfig nearCacheConfig,
                                                         NearCacheStatsImpl nearCacheStats,
                                                         HiDensityStorageInfo storageInfo,
                                                         EnterpriseSerializationService ss,
                                                         ClassLoader classLoader) {
            super(nearCacheConfig, nearCacheStats, storageInfo, ss, classLoader);
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
                currentThread().interrupt();
                return null;
            }
        }

        @Override
        public void put(K key, Data keyData, V value) {
            lock.lock();
            try {
                super.put(key, keyData, value);
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

        @Override
        public long tryReserveForUpdate(K key, Data keyData) {
            lock.lock();
            try {
                return super.tryReserveForUpdate(key, keyData);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public V tryPublishReserved(K key, V value, long reservationId, boolean deserialize) {
            lock.lock();
            try {
                return super.tryPublishReserved(key, value, reservationId, deserialize);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public StaleReadDetector getStaleReadDetector() {
            lock.lock();
            try {
                return super.getStaleReadDetector();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Iterator<Data> getKeySetIterator() {
            return records.keySet().iterator();
        }

        @Override
        public void lock() {
            lock.lock();
        }

        @Override
        public void unlock() {
            lock.unlock();
        }
    }
}
