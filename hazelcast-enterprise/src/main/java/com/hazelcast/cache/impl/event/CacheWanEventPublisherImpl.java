package com.hazelcast.cache.impl.event;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.CacheEventContext;
import com.hazelcast.cache.impl.CacheEventContextUtil;
import com.hazelcast.nio.serialization.Data;

/**
 * Provides methods to publish wan replication
 * update/remove methods and their backups.
 */
public class CacheWanEventPublisherImpl implements CacheWanEventPublisher {

    private final EnterpriseCacheService cacheService;

    public CacheWanEventPublisherImpl(EnterpriseCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void publishWanReplicationUpdate(String cacheName, CacheEntryView<Data, Data> entryView) {
        CacheEventContext cacheEventContext = CacheEventContextUtil.createCacheUpdatedEvent(entryView.getKey(),
                entryView.getValue(), null, entryView.getExpirationTime(), entryView.getLastAccessTime(),
                entryView.getAccessHit());
        cacheEventContext.setCacheName(cacheName);
        cacheService.publishWanEvent(cacheEventContext);
    }

    @Override
    public void publishWanReplicationUpdateBackup(String cacheName, CacheEntryView<Data, Data> entryView) {
        CacheEventContext cacheEventContext = CacheEventContextUtil.createCacheUpdatedEvent(entryView.getKey(),
                entryView.getValue(), null, entryView.getExpirationTime(), entryView.getLastAccessTime(),
                entryView.getAccessHit());
        cacheEventContext.setCacheName(cacheName);
        cacheService.publishWanEventBackup(cacheEventContext);
    }

    @Override
    public void publishWanReplicationRemove(String cacheName, Data key) {
        CacheEventContext cacheEventContext = CacheEventContextUtil.createCacheRemovedEvent(key);
        cacheEventContext.setCacheName(cacheName);
        cacheService.publishWanEvent(cacheEventContext);
    }

    @Override
    public void publishWanReplicationRemoveBackup(String cacheName, Data key) {
        CacheEventContext cacheEventContext = CacheEventContextUtil.createCacheRemovedEvent(key);
        cacheEventContext.setCacheName(cacheName);
        cacheService.publishWanEventBackup(cacheEventContext);
    }
}
