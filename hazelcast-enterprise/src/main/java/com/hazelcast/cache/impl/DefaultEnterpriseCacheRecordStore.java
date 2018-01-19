package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;

/**
 * The {@link EnterpriseCacheRecordStore} implementation that provides
 * merge function implementation.
 */
public class DefaultEnterpriseCacheRecordStore extends CacheRecordStore
        implements EnterpriseCacheRecordStore {

    public DefaultEnterpriseCacheRecordStore(String name, int partitionId, NodeEngine nodeEngine,
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
        Data value = cacheEntryView.getValue();
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
                                          new DefaultCacheEntryView(key,
                                                                    toData(record.getValue()),
                                                                    record.getCreationTime(),
                                                                    record.getExpirationTime(),
                                                                    record.getLastAccessTime(),
                                                                    record.getAccessHit()));
                if (record.getValue() != newValue) {
                    merged = updateRecordWithExpiry(key, newValue, record, expiryTime,
                                                    now, true, completionId, caller, origin);
                }
                publishEvent(createCacheCompleteEvent(key, CacheRecord.TIME_NOT_AVAILABLE,
                                                      origin, completionId));
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
