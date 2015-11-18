package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordHashMap;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.nio.serialization.Data;

/**
 * Hot-restart variant of CacheRecordHashMap.
 * <p/>
 * Eviction methods don't actually evict the record but only
 * clears the value contained in the record.
 */
public class HotRestartCacheRecordHashMap extends CacheRecordHashMap {

    public HotRestartCacheRecordHashMap(int initialCapacity, CacheContext cacheContext) {
        super(initialCapacity, cacheContext);
    }

    @Override
    public <C extends EvictionCandidate<Data, CacheRecord>> int evict(Iterable<C> evictionCandidates,
            EvictionListener<Data, CacheRecord> evictionListener) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, CacheRecord> evictionCandidate : evictionCandidates) {
            CacheRecord record = get(evictionCandidate.getAccessor());
            if (record != null) {
                actualEvictedCount++;
                if (evictionListener != null) {
                    evictionListener.onEvict(evictionCandidate.getAccessor(), evictionCandidate.getEvictable());
                }
                record.setValue(null);
            }
        }
        return actualEvictedCount;
    }

    @Override
    protected boolean isValidForFetching(CacheRecord record, long now) {
        return !record.isExpiredAt(now) && !record.isTombstone();
    }

    @Override
    protected boolean isValidForSampling(CacheRecord record) {
        return !record.isTombstone();
    }
}
