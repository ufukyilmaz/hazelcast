package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.ExpirationChecker;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Hot-restart variant of HiDensityNativeMemoryCacheRecordMap.
 * <p/>
 * All mutator actions are synchronized to make them visible to
 * hot-restart GC thread.
 * <p/>
 * Eviction methods don't actually evict the record but only
 * clears and disposes the value contained in the record.
 */
public class HotRestartHiDensityNativeMemoryCacheRecordMap
        extends HiDensityNativeMemoryCacheRecordMap {

    // Hot-restart GC thread needs to read internal record map.
    // That's why modification of record map itself and modification of
    // records in this map should be synchronized with GC thread's
    // reads of record map and its records.
    // This mutex guards all modifications and reads done by GC thread.
    private final Object mutex = new Object();

    private final HiDensityNativeMemoryCacheRecord recordHolder;

    public HotRestartHiDensityNativeMemoryCacheRecordMap(int initialCapacity,
            HiDensityRecordProcessor cacheRecordProcessor, HiDensityStorageInfo cacheInfo) {
        super(initialCapacity, cacheRecordProcessor, cacheInfo);

        recordHolder = new HiDensityNativeMemoryCacheRecord(cacheRecordProcessor);
    }

    // All mutator methods should be in sync block
    Object getMutex() {
        return mutex;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord put(Data key, HiDensityNativeMemoryCacheRecord value) {
        synchronized (mutex) {
            return super.put(key, value);
        }
    }

    @Override
    public boolean set(Data key, HiDensityNativeMemoryCacheRecord value) {
        synchronized (mutex) {
            return super.set(key, value);
        }
    }

    @Override
    public HiDensityNativeMemoryCacheRecord putIfAbsent(Data key, HiDensityNativeMemoryCacheRecord value) {
        synchronized (mutex) {
            return super.putIfAbsent(key, value);
        }
    }

    @Override
    public HiDensityNativeMemoryCacheRecord replace(Data key, HiDensityNativeMemoryCacheRecord value) {
        synchronized (mutex) {
            return super.replace(key, value);
        }
    }

    @Override
    public boolean delete(Data key) {
        synchronized (mutex) {
            return super.delete(key);
        }
    }

    @Override
    public boolean remove(Object k, Object v) {
        synchronized (mutex) {
            return super.remove(k, v);
        }
    }

    @Override
    public boolean replace(Data key, HiDensityNativeMemoryCacheRecord oldValue,
            HiDensityNativeMemoryCacheRecord newValue) {
        synchronized (mutex) {
            return super.replace(key, oldValue, newValue);
        }
    }

    @Override
    public HiDensityNativeMemoryCacheRecord remove(Object k) {
        synchronized (mutex) {
            return super.remove(k);
        }
    }

    @Override
    protected boolean isValidForSampling(int slot) {
        if (!isAssigned(slot)) {
            return false;
        }
        recordHolder.reset(getValue(slot));
        return !recordHolder.isTombstone();
    }

    // Overridden to avoid disposing key and record itself.
    // Only value will be disposed.
    // Key and record itself will be removed
    // when hot-restart GC thread instructs to release tombstones.
    @Override
    protected int forceEvict(KeyIter iterator,
            EvictionListener<Data, HiDensityNativeMemoryCacheRecord> evictionListener, int evictCount) {

        int evictedEntryCount = 0;
        HiDensityNativeMemoryCacheRecord record = recordHolder;
        while (iterator.hasNext()) {
            iterator.nextSlot();

            int slot = iterator.getCurrentSlot();
            record.reset(getValue(slot));

            if (record.isTombstone()) {
                continue;
            }

            synchronized (mutex) {
                recordProcessor.disposeValue(record);
            }

            if (evictionListener != null) {
                keyHolder.reset(getKey(slot));
                evictionListener.onEvict(keyHolder, record);
            }

            if (++evictedEntryCount >= evictCount) {
                break;
            }
        }
        recordHolder.reset(NULL_ADDRESS);
        return evictedEntryCount;
    }

    @Override
    protected <C extends EvictionCandidate<Data, HiDensityNativeMemoryCacheRecord>> int doEvictExpiredRecords(
            EvictionListener<Data, HiDensityNativeMemoryCacheRecord> evictionListener,
            ExpirationChecker<HiDensityNativeMemoryCacheRecord> expirationChecker) {

        // not expected to reach here
        throw new UnsupportedOperationException();
    }

    // Overridden to avoid disposing key and record itself.
    // Only value will be disposed.
    // Key and record itself will be removed
    // when hot-restart GC thread instructs to release tombstones.
    @Override
    public <C extends EvictionCandidate<Data, HiDensityNativeMemoryCacheRecord>> int evict(
            Iterable<C> evictionCandidates, EvictionListener<Data, HiDensityNativeMemoryCacheRecord> evictionListener) {

        if (evictionCandidates == null) {
            return 0;
        }

        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, HiDensityNativeMemoryCacheRecord> evictionCandidate : evictionCandidates) {
            Data key = evictionCandidate.getAccessor();
            HiDensityNativeMemoryCacheRecord record = get(key);
            if (record != null) {
                actualEvictedCount++;

                synchronized (mutex) {
                    recordProcessor.disposeValue(record);
                }

                if (evictionListener != null) {
                    evictionListener.onEvict(key, record);
                }
            }
        }
        return actualEvictedCount;
    }

    @Override
    public void putAll(Map<? extends Data, ? extends HiDensityNativeMemoryCacheRecord> map) {
        synchronized (mutex) {
            super.putAll(map);
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            super.clear();
        }
    }

    @Override
    public void dispose() {
        synchronized (mutex) {
            super.dispose();
        }
    }
}
