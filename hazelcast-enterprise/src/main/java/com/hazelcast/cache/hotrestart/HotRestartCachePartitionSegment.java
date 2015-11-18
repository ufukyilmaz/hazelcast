package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;

import static com.hazelcast.spi.hotrestart.CacheDescriptor.isProvisionalName;
import static com.hazelcast.spi.hotrestart.CacheDescriptor.toProvisionalName;

/**
 * Cache partition segment which supports the activation of hot-restarted record stores.
 */
public class HotRestartCachePartitionSegment extends CachePartitionSegment {
    public HotRestartCachePartitionSegment(EnterpriseCacheService cacheService, int partitionId) {
        super(cacheService, partitionId);
    }

    @Override public ICacheRecordStore createNew(String name) {
        final AbstractCacheRecordStore created = (AbstractCacheRecordStore) super.createNew(name);
        if (!isProvisionalName(name)) {
            AbstractCacheRecordStore provisional =
                    (AbstractCacheRecordStore) recordStores.remove(toProvisionalName(name));
            if (provisional != null) {
                created.transferRecordsFrom(provisional);
            }
        }
        return created;
    }

    @Override
    public ICacheRecordStore getRecordStore(String name) {
        AbstractCacheRecordStore recordStore = (AbstractCacheRecordStore) super.getRecordStore(name);
        if (recordStore == null && !isProvisionalName(name)) {
            CacheConfig cacheConfig = cacheService.getCacheConfig(name);
            if (cacheConfig.isHotRestartEnabled()) {
                return getOrCreateRecordStore(name);
            }
        }
        return recordStore;
    }
}
