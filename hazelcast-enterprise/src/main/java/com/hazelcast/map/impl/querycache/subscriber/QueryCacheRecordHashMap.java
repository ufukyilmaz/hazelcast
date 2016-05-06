package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
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

    private final SerializationService serializationService;

    public QueryCacheRecordHashMap(SerializationService serializationService, int initialCapacity) {
        super(initialCapacity);
        this.serializationService = serializationService;
    }

    public QueryCacheRecordHashMap(SerializationService serializationService,
                                   int initialCapacity, float loadFactor, int concurrencyLevel,
                                   ConcurrentReferenceHashMap.ReferenceType keyType,
                                   ConcurrentReferenceHashMap.ReferenceType valueType,
                                   EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
        this.serializationService = serializationService;
    }

    /**
     * @see com.hazelcast.util.SampleableConcurrentHashMap.SamplingEntry
     * @see EvictionCandidate
     */
    public class QueryCacheEvictableSamplingEntry
            extends SamplingEntry<Data, QueryCacheRecord>
            implements EvictionCandidate {

        public QueryCacheEvictableSamplingEntry(Data key, QueryCacheRecord value) {
            super(key, value);
        }

        @Override
        public Object getAccessor() {
            return key;
        }

        @Override
        public Evictable getEvictable() {
            return value;
        }

        @Override
        public Object getKey() {
            return serializationService.toObject(key);
        }

        @Override
        public Object getValue() {
            return serializationService.toObject(value.getValue());
        }

        @Override
        public long getCreationTime() {
            return value.getCreationTime();
        }

        @Override
        public long getLastAccessTime() {
            return value.getLastAccessTime();
        }

        @Override
        public long getAccessHit() {
            return value.getAccessHit();
        }

    }

    @Override
    protected QueryCacheEvictableSamplingEntry createSamplingEntry(Data key, QueryCacheRecord value) {
        return new QueryCacheEvictableSamplingEntry(key, value);
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
    public Iterable<QueryCacheEvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }

}
