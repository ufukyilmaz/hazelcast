package com.hazelcast.cache;

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
        CacheService service = getService();
        CacheRecordStore cache = service.getCache(name, getPartitionId());
        if (cache != null) {
            cache.clear();
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR;
    }
}
