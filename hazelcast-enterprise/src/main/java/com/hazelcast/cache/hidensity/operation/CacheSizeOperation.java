package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.spi.ReadonlyOperation;

/**
 * @author mdogan 06/02/14
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
