package com.hazelcast.cache;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;

/**
 * @author mdogan 06/02/14
 */
public class CacheClearOperation extends PartitionWideCacheOperation {

    public CacheClearOperation() {
    }

    public CacheClearOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        ICacheService service = getService();
        ICacheRecordStore cache = service.getCache(name, getPartitionId());
        if (cache != null) {
            cache.clear();
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR;
    }
}
