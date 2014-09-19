package com.hazelcast.cache;

/**
 * @author mdogan 06/02/14
 */
public class CacheSizeOperation extends PartitionWideCacheOperation {

    public CacheSizeOperation() {
    }

    public CacheSizeOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        CacheService service = getService();
        CacheRecordStore cache = service.getCache(name, getPartitionId());
        response = cache != null ? cache.size() : 0;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.SIZE;
    }
}
