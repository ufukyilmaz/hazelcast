package com.hazelcast.cache;

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.merge.CacheMergePolicy;
import com.hazelcast.cache.wan.CacheEntryView;
import com.hazelcast.cache.wan.SimpleCacheEntryView;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;

/**
 * The {@link com.hazelcast.cache.EnterpriseCacheRecordStore} implementation that provides
 * merge function implementation
 */
public class EnterpriseCacheRecordStoreImpl extends CacheRecordStore
        implements EnterpriseCacheRecordStore {

    public EnterpriseCacheRecordStoreImpl(String name, int partitionId, NodeEngine nodeEngine,
                                          AbstractCacheService cacheService) {
        super(name, partitionId, nodeEngine, cacheService);
    }

    @Override
    public boolean merge(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy,
                         String caller, int completionId, String origin) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean merged = false;
        Data key = cacheEntryView.getKey();
        Object value = cacheEntryView.getValue();
        long expiryTime = cacheEntryView.getExpirationTime();
        CacheRecord record = records.get(key);

        boolean isExpired = processExpiredEntry(key, record, now);
        try {
            if (record == null || isExpired) {
                merged = createRecordWithExpiry(key, value, cacheEntryView.getExpirationTime(),
                                                now, true, completionId, origin) != null;
            } else {
                Object newValue =
                        mergePolicy.merge(name,
                                          cacheEntryView,
                                          new SimpleCacheEntryView(key, record.getValue(),
                                                  record.getExpirationTime(), record.getAccessHit()));
                if (record.getValue() != newValue) {
                    merged = updateRecordWithExpiry(key, newValue, record, expiryTime,
                                                    now, true, completionId, caller, origin);
                }
                publishEvent(createCacheCompleteEvent(key,
                        CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE, origin, completionId));
            }

            if (merged && isStatisticsEnabled()) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }

            return merged;
        } catch (Throwable error) {
            throw ExceptionUtil.rethrow(error);
        }
    }

}
