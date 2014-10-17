package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;

/**
 * TODO add a proper JavaDoc
 */
public class OffHeapOperationProvider implements CacheOperationProvider {

    private final String nameWithPrefix;

    public OffHeapOperationProvider(String nameWithPrefix) {
        this.nameWithPrefix = nameWithPrefix;
    }

    @Override
    public Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get) {
        return new CachePutOperation(nameWithPrefix, key, value, policy, get);
    }

    @Override
    public Operation createGetOperation(Data key, ExpiryPolicy policy) {
        return new CacheGetOperation(nameWithPrefix, key, policy);
    }
}
