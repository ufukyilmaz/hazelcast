package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.spi.ReadonlyOperation;

/**
 * Calculates the entry size of this store which reflects the partition size of the cache.
 */
public class CacheSizeOperation
        extends AbstractHiDensityCacheOperation
        implements ReadonlyOperation {

    public CacheSizeOperation() {
    }

    public CacheSizeOperation(String name) {
        super(name, true);
    }

    @Override
    protected void runInternal() throws Exception {
        response = cache != null ? cache.size() : 0;
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.SIZE;
    }
}
