package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.map.impl.querycache.subscriber.record.DataQueryCacheRecordFactory;
import com.hazelcast.map.impl.querycache.subscriber.record.ObjectQueryCacheRecordFactory;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link QueryCacheRecordStore}
 *
 * @see QueryCacheRecordStore
 */
class DefaultQueryCacheRecordStore implements QueryCacheRecordStore {

    private static final int DEFAULT_CACHE_CAPACITY = 1000;
    private final QueryCacheRecordHashMap cache;
    private final QueryCacheRecordFactory recordFactory;
    private final IndexService indexService;
    private final SerializationService serializationService;
    private final EvictionOperator evictionOperator;

    public DefaultQueryCacheRecordStore(SerializationService serializationService,
                                        IndexService indexService,
                                        QueryCacheConfig config, EvictionListener listener) {
        this.cache = new QueryCacheRecordHashMap(DEFAULT_CACHE_CAPACITY);
        this.serializationService = serializationService;
        this.recordFactory = getRecordFactory(config.getInMemoryFormat());
        this.indexService = indexService;
        this.evictionOperator = new EvictionOperator(cache, config, listener);
    }

    private QueryCacheRecord accessRecord(QueryCacheRecord record) {
        if (record == null) {
            return null;
        }
        record.incrementAccessHit();
        record.setAccessTime(Clock.currentTimeMillis());
        return record;
    }

    private QueryCacheRecordFactory getRecordFactory(InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
            case BINARY:
                return new DataQueryCacheRecordFactory(serializationService);
            case OBJECT:
                return new ObjectQueryCacheRecordFactory(serializationService);
            default:
                throw new IllegalArgumentException("Not a known format [" + inMemoryFormat + "]");
        }
    }

    @Override
    public QueryCacheRecord add(Data keyData, Data valueData) {
        evictionOperator.evictIfRequired();

        QueryCacheRecord entry = recordFactory.createEntry(keyData, valueData);
        saveIndex(keyData, entry);

        return cache.put(keyData, entry);
    }

    private void saveIndex(Data keyData, QueryCacheRecord entry) {
        if (indexService.hasIndex()) {
            Object currentValue = entry.getValue();
            QueryEntry queryEntry = new QueryEntry(serializationService,
                    keyData, keyData, currentValue);
            indexService.saveEntryIndex(queryEntry);
        }
    }

    @Override
    public QueryCacheRecord get(Data keyData) {
        QueryCacheRecord record = cache.get(keyData);
        return accessRecord(record);
    }

    @Override
    public QueryCacheRecord remove(Data keyData) {
        QueryCacheRecord oldRecord = cache.remove(keyData);
        removeIndex(keyData);
        return oldRecord;
    }

    private void removeIndex(Data keyData) {
        if (indexService.hasIndex()) {
            indexService.removeEntryIndex(keyData);
        }
    }

    @Override
    public boolean containsKey(Data keyData) {
        QueryCacheRecord record = get(keyData);
        return record != null;
    }

    @Override
    public boolean containsValue(Object value) {
        Collection<QueryCacheRecord> values = cache.values();
        for (QueryCacheRecord cacheRecord : values) {
            Object cacheRecordValue = cacheRecord.getValue();
            if (recordFactory.isEquals(cacheRecordValue, value)) {
                accessRecord(cacheRecord);
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Data> keySet() {
        return cache.keySet();
    }

    @Override
    public Set<Map.Entry<Data, QueryCacheRecord>> entrySet() {
        return cache.entrySet();
    }

    @Override
    public int clear() {
        int removeCount = 0;
        Set<Data> dataKeys = keySet();
        for (Data dataKey : dataKeys) {
            QueryCacheRecord oldRecord = remove(dataKey);
            if (oldRecord != null) {
                removeCount++;
            }
        }
        return removeCount;
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public int size() {
        return cache.size();
    }
}
