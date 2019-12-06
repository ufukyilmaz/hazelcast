package com.hazelcast.cache.impl.event;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheEventContext;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheRemovedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheUpdatedEvent;

/**
 * Provides methods to publish WAN replication
 * update/remove methods and their backups.
 */
public class CacheWanEventPublisherImpl implements CacheWanEventPublisher {

    private final EnterpriseCacheService cacheService;

    public CacheWanEventPublisherImpl(EnterpriseCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void publishWanUpdate(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView) {
        CacheEventContext cacheUpdatedEvent = createCacheUpdatedEvent(entryView.getKey(),
                entryView.getValue(), null,
                entryView.getCreationTime(), entryView.getExpirationTime(),
                entryView.getLastAccessTime(), entryView.getHits(), (Data) entryView.getExpiryPolicy())
                .setCacheName(cacheNameWithPrefix);

        cacheService.publishWanEvent(cacheUpdatedEvent);
    }

    @Override
    public void publishWanRemove(String cacheNameWithPrefix, Data key) {
        CacheEventContext cacheRemovedEvent = createCacheRemovedEvent(key)
                .setCacheName(cacheNameWithPrefix);

        cacheService.publishWanEvent(cacheRemovedEvent);
    }
}
