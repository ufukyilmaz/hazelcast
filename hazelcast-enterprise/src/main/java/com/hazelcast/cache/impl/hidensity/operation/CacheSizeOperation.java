package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

/**
 * Calculates the entry size of this store which reflects the partition size of the cache.
 */
public class CacheSizeOperation
        extends HiDensityCacheOperation implements ReadonlyOperation {

    public CacheSizeOperation() {
    }

    public CacheSizeOperation(String name) {
        super(name, true);
    }

    @Override
    protected void runInternal() {
        response = recordStore != null ? recordStore.size() : 0;
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.SIZE;
    }
}
