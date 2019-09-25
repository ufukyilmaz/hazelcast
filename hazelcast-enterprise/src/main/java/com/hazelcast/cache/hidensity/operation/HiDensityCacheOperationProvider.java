package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheRemoveAllOperationFactory;
import com.hazelcast.cache.impl.operation.EnterpriseCacheOperationProvider;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Provide operations for {@link InMemoryFormat#NATIVE}.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class HiDensityCacheOperationProvider extends EnterpriseCacheOperationProvider {

    public HiDensityCacheOperationProvider(String nameWithPrefix) {
        super(nameWithPrefix);
    }

    @Override
    public Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get, int completionId) {
        CachePutOperation cachePutOperation = new CachePutOperation(nameWithPrefix, key, value, policy, get);
        cachePutOperation.setCompletionId(completionId);
        return cachePutOperation;
    }

    @Override
    public Operation createPutAllOperation(List<Map.Entry<Data, Data>> entries, ExpiryPolicy policy, int completionId) {
        CachePutAllOperation cachePutAllOperation = new CachePutAllOperation(nameWithPrefix, entries, policy);
        cachePutAllOperation.setCompletionId(completionId);
        return cachePutAllOperation;
    }

    @Override
    public Operation createGetOperation(Data key, ExpiryPolicy policy) {
        return new CacheGetOperation(nameWithPrefix, key, policy);
    }

    @Override
    public Operation createSetExpiryPolicyOperation(List<Data> keys, Data expiryPolicy) {
        return new CacheSetExpiryPolicyOperation(nameWithPrefix, keys, expiryPolicy);
    }

    @Override
    public Operation createContainsKeyOperation(Data key) {
        return new CacheContainsKeyOperation(nameWithPrefix, key);
    }

    @Override
    public Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        CachePutIfAbsentOperation cachePutIfAbsentOperation = new CachePutIfAbsentOperation(nameWithPrefix, key, value, policy);
        cachePutIfAbsentOperation.setCompletionId(completionId);
        return cachePutIfAbsentOperation;
    }

    @Override
    public Operation createRemoveOperation(Data key, Data value, int completionId) {
        CacheRemoveOperation cacheRemoveOperation = new CacheRemoveOperation(nameWithPrefix, key, value);
        cacheRemoveOperation.setCompletionId(completionId);
        return cacheRemoveOperation;
    }

    @Override
    public Operation createGetAndRemoveOperation(Data key, int completionId) {
        CacheGetAndRemoveOperation cacheGetAndRemoveOperation = new CacheGetAndRemoveOperation(nameWithPrefix, key);
        cacheGetAndRemoveOperation.setCompletionId(completionId);
        return cacheGetAndRemoveOperation;
    }

    @Override
    public Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy, int completionId) {
        CacheReplaceOperation cacheReplaceOperation = new CacheReplaceOperation(nameWithPrefix, key, oldValue, newValue, policy);
        cacheReplaceOperation.setCompletionId(completionId);
        return cacheReplaceOperation;
    }

    @Override
    public Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        CacheGetAndReplaceOperation getAndReplaceOperation = new CacheGetAndReplaceOperation(nameWithPrefix, key, value, policy);
        getAndReplaceOperation.setCompletionId(completionId);
        return getAndReplaceOperation;
    }

    @Override
    public Operation createEntryProcessorOperation(Data key, Integer completionId,
                                                   EntryProcessor entryProcessor, Object... args) {
        return new CacheEntryProcessorOperation(nameWithPrefix, key, completionId, entryProcessor, args);
    }

    @Override
    public Operation createWanRemoveOperation(UUID origin, Data key, int completionId) {
        return new WanCacheRemoveOperation(nameWithPrefix, origin, key, completionId);
    }

    @Override
    public Operation createWanMergeOperation(CacheMergeTypes mergingEntry,
                                             SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy, int completionId) {
        return new WanCacheMergeOperation(nameWithPrefix, mergePolicy, mergingEntry, completionId);
    }

    @Override
    public Operation createMergeOperation(String name, List<CacheMergeTypes> mergingEntries,
                                          SplitBrainMergePolicy<Data, CacheMergeTypes> policy) {
        return new CacheMergeOperation(name, mergingEntries, policy);
    }

    @Override
    public OperationFactory createMergeOperationFactory(String name, int[] partitions, List<CacheMergeTypes>[] mergingEntries,
                                                        SplitBrainMergePolicy<Data, CacheMergeTypes> policy) {
        return new CacheMergeOperationFactory(name, partitions, mergingEntries, policy);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy) {
        return new CacheGetAllOperationFactory(nameWithPrefix, keySet, policy);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues) {
        return new CacheLoadAllOperationFactory(nameWithPrefix, keySet, replaceExistingValues);
    }

    @Override
    public OperationFactory createClearOperationFactory() {
        return new CacheClearOperationFactory(nameWithPrefix);
    }

    @Override
    public OperationFactory createRemoveAllOperationFactory(Set<Data> keySet, Integer completionId) {
        return new CacheRemoveAllOperationFactory(nameWithPrefix, keySet, completionId);
    }

    @Override
    public OperationFactory createSizeOperationFactory() {
        return new CacheSizeOperationFactory(nameWithPrefix);
    }
}
