package com.hazelcast.cache;

import com.hazelcast.cache.enterprise.operation.CacheDestroyOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Set;

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

    @Override
    public Operation createContainsKeyOperation(Data key) {
        return new CacheContainsKeyOperation(nameWithPrefix, key);
    }

    @Override
    public Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy) {
        return new CachePutIfAbsentOperation(nameWithPrefix, key, value, policy);
    }

    @Override
    public Operation createRemoveOperation(Data key, Data value) {
        return new CacheRemoveOperation(nameWithPrefix, key, value);
    }

    @Override
    public Operation createGetAndRemoveOperation(Data key) {
        return new CacheGetAndRemoveOperation(nameWithPrefix, key);
    }

    @Override
    public Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy) {
        return new CacheReplaceOperation(nameWithPrefix, key, oldValue, newValue, policy);
    }

    @Override
    public Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy) {
        return new CacheGetAndReplaceOperation(nameWithPrefix, key, value, policy);
    }

    @Override
    public Operation createEntryProcessorOperation(Data key, Integer completionId, EntryProcessor entryProcessor, Object... args) {
        throw new UnsupportedOperationException("implement entry processor operation !!!");
    }

    @Override
    public Operation createKeyIteratorOperation(int lastTableIndex, int fetchSize) {
        throw new UnsupportedOperationException("implement key iterator operation !!!");
    }

    @Override
    public OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy) {
        throw new UnsupportedOperationException("implement get all operation !!!");
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues) {
        throw new UnsupportedOperationException("implement load all operation !!!");
    }

    @Override
    public OperationFactory createClearOperationFactory(Set<Data> keySet, boolean isRemoveAll, Integer completionId) {
        return new CacheClearOperationFactory(nameWithPrefix, keySet, isRemoveAll, completionId);
    }

    @Override
    public OperationFactory createSizeOperationFactory() {
        return new CacheSizeOperationFactory(nameWithPrefix);
    }
}
