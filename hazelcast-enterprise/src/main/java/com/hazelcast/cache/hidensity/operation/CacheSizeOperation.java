package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * @author mdogan 06/02/14
 */
public class CacheSizeOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation {

    public CacheSizeOperation() {
    }

    public CacheSizeOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        CacheService service = getService();
        ICacheRecordStore cache = service.getRecordStore(name, getPartitionId());
        response = cache != null ? cache.size() : 0;
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.SIZE;
    }
}
