package com.hazelcast.cache.impl.event;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheEventContext;
import com.hazelcast.cache.impl.CacheEventContextUtil;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.nio.serialization.Data;

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
    public void publishWanReplicationUpdate(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView) {
        CacheEventContext cacheEventContext =
                CacheEventContextUtil.createCacheUpdatedEvent(
                        entryView.getKey(), entryView.getValue(), null,
                        entryView.getCreationTime(), entryView.getExpirationTime(),
                        entryView.getLastAccessTime(), entryView.getAccessHit());
        cacheEventContext.setCacheName(cacheNameWithPrefix);
        cacheService.publishWanEvent(cacheEventContext);
    }

    @Override
    public void publishWanReplicationUpdateBackup(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView) {
        CacheEventContext cacheEventContext =
                CacheEventContextUtil.createCacheUpdatedEvent(
                        entryView.getKey(), entryView.getValue(), null,
                        entryView.getCreationTime(), entryView.getExpirationTime(),
                        entryView.getLastAccessTime(), entryView.getAccessHit());
        cacheEventContext.setCacheName(cacheNameWithPrefix);
        cacheService.publishWanEventBackup(cacheEventContext);
    }

    @Override
    public void publishWanReplicationRemove(String cacheNameWithPrefix, Data key) {
        CacheEventContext cacheEventContext = CacheEventContextUtil.createCacheRemovedEvent(key);
        cacheEventContext.setCacheName(cacheNameWithPrefix);
        cacheService.publishWanEvent(cacheEventContext);
    }

    @Override
    public void publishWanReplicationRemoveBackup(String cacheNameWithPrefix, Data key) {
        CacheEventContext cacheEventContext = CacheEventContextUtil.createCacheRemovedEvent(key);
        cacheEventContext.setCacheName(cacheNameWithPrefix);
        cacheService.publishWanEventBackup(cacheEventContext);
    }
}
