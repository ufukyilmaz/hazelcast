package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains helper methods for {@link InternalQueryCache} main implementation.
 *
 * @param <K> the key type for this {@link InternalQueryCache}
 * @param <V> the value type for this {@link InternalQueryCache}
 */
abstract class AbstractInternalQueryCache<K, V> implements InternalQueryCache<K, V> {

    protected final boolean includeValue;
    protected final String mapName;
    protected final String cacheName;
    protected final String userGivenCacheName;
    protected final IMap delegate;
    protected final QueryCacheContext context;
    protected final QueryCacheRecordStore recordStore;
    protected final IndexService indexService;
    protected final SerializationService serializationService;
    protected final PartitioningStrategy partitioningStrategy;

    public AbstractInternalQueryCache(String cacheName, String userGivenCacheName, IMap delegate, QueryCacheContext context) {
        this.cacheName = cacheName;
        this.userGivenCacheName = userGivenCacheName;
        this.mapName = delegate.getName();
        this.delegate = delegate;
        this.context = context;
        this.indexService = new IndexService();
        this.serializationService = context.getSerializationService();
        this.includeValue = isIncludeValue();
        this.partitioningStrategy = getPartitioningStrategy();
        this.recordStore = new DefaultQueryCacheRecordStore(serializationService,
                indexService, getQueryCacheConfig(), getEvictionListener());

    }

    protected Predicate getPredicate() {
        return getQueryCacheConfig().getPredicateConfig().getImplementation();
    }

    private QueryCacheConfig getQueryCacheConfig() {
        QueryCacheConfigurator queryCacheConfigurator = context.getQueryCacheConfigurator();
        return queryCacheConfigurator.getOrCreateConfiguration(mapName, userGivenCacheName);
    }

    private EvictionListener getEvictionListener() {
        return new EvictionListener<Data, QueryCacheRecord>() {
            @Override
            public void onEvict(Data dataKey, QueryCacheRecord record) {
                EventPublisherHelper.publishEntryEvent(context, mapName,
                        cacheName, dataKey, null, record, EntryEventType.EVICTED);
            }
        };
    }


    PartitioningStrategy getPartitioningStrategy() {
        if (delegate instanceof MapProxyImpl) {
            return ((MapProxyImpl) delegate).getPartitionStrategy();
        }
        return null;
    }

    protected void doFullKeyScan(Predicate predicate, Set<K> resultingSet) {
        SerializationService serializationService = this.serializationService;

        QueryEntry queryEntry = new QueryEntry();
        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();

            queryEntry.init(serializationService, keyData, keyData, value);

            boolean valid = predicate.apply(queryEntry);
            if (valid) {
                resultingSet.add((K) queryEntry.getKey());
            }
        }
    }

    protected void doFullEntryScan(Predicate predicate, Set<Map.Entry<K, V>> resultingSet) {
        SerializationService serializationService = this.serializationService;

        QueryEntry queryEntry = new QueryEntry();
        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();

            queryEntry.init(serializationService, keyData, keyData, value);

            boolean valid = predicate.apply(queryEntry);
            if (valid) {
                Object keyObject = queryEntry.getKey();
                Object valueObject = queryEntry.getValue();
                Map.Entry simpleEntry = new AbstractMap.SimpleEntry(keyObject, valueObject);
                resultingSet.add(simpleEntry);
            }
        }
    }

    protected void doFullValueScan(Predicate predicate, Set<V> resultingSet) {
        SerializationService serializationService = this.serializationService;

        QueryEntry queryEntry = new QueryEntry();
        Set<Map.Entry<Data, QueryCacheRecord>> entries = recordStore.entrySet();
        for (Map.Entry<Data, QueryCacheRecord> entry : entries) {
            Data keyData = entry.getKey();
            QueryCacheRecord record = entry.getValue();
            Object value = record.getValue();

            queryEntry.init(serializationService, keyData, keyData, value);

            boolean valid = predicate.apply(queryEntry);
            if (valid) {
                Object valueObject = queryEntry.getValue();
                resultingSet.add((V) valueObject);
            }
        }
    }

    private boolean isIncludeValue() {
        QueryCacheConfig config = getQueryCacheConfig();
        return config.isIncludeValue();
    }

    protected QueryCacheEventService getEventService() {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        return subscriberContext.getEventService();
    }


    protected <T> T toObject(Object valueInRecord) {
        return serializationService.toObject(valueInRecord);
    }

    protected Data toData(Object key) {
        return serializationService.toData(key, partitioningStrategy);
    }

}
