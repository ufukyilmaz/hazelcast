package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.cache.impl.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.SampleableConcurrentHashMap;

import java.util.EnumSet;

/**
 * Evictable concurrent hash map implementation.
 *
 * @see SampleableConcurrentHashMap
 * @see SampleableEvictableStore
 */
public class QueryCacheRecordHashMap extends SampleableConcurrentHashMap<Data, QueryCacheRecord>
        implements SampleableEvictableStore<Data, QueryCacheRecord> {

    public QueryCacheRecordHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public QueryCacheRecordHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
                                   ConcurrentReferenceHashMap.ReferenceType keyType,
                                   ConcurrentReferenceHashMap.ReferenceType valueType,
                                   EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

    /**
     * @see com.hazelcast.util.SampleableConcurrentHashMap.SamplingEntry
     * @see EvictionCandidate
     */
    public class EvictableSamplingEntry extends SamplingEntry implements EvictionCandidate {

        public EvictableSamplingEntry(Data key, QueryCacheRecord value) {
            super(key, value);
        }

        @Override
        public Object getAccessor() {
            return getKey();
        }

        @Override
        public Evictable getEvictable() {
            return getValue();
        }

    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(Data key, QueryCacheRecord value) {
        return new EvictableSamplingEntry(key, value);
    }

    @Override
    public <C extends EvictionCandidate<Data, QueryCacheRecord>>
    int evict(Iterable<C> evictionCandidates, EvictionListener<Data, QueryCacheRecord> evictionListener) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, QueryCacheRecord> evictionCandidate : evictionCandidates) {
            if (remove(evictionCandidate.getAccessor()) != null) {
                actualEvictedCount++;
                if (evictionListener != null) {
                    evictionListener.onEvict(evictionCandidate.getAccessor(), evictionCandidate.getEvictable());
                }
            }
        }
        return actualEvictedCount;
    }

    @Override
    public Iterable<EvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }

}
