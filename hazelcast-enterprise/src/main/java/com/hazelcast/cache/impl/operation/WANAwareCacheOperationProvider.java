package com.hazelcast.cache.impl.operation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.wan.WanReplicationPublisher;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * This class is responsible for WAN replication related
 * checks for supported mutating operations. {@link
 * com.hazelcast.spi.Operation} creations are delegated to underlying
 * {@link EnterpriseCacheOperationProvider} instance after checks.
 */
public class WANAwareCacheOperationProvider extends EnterpriseCacheOperationProvider {

    private final EnterpriseCacheOperationProvider delegate;
    private final Function<String, WanReplicationPublisher> publisherLookupFunction;

    public WANAwareCacheOperationProvider(String nameWithPrefix,
                                          EnterpriseCacheOperationProvider delegate,
                                          Function<String, WanReplicationPublisher> publisherLookupFunction) {
        super(nameWithPrefix);
        this.delegate = delegate;
        this.publisherLookupFunction = publisherLookupFunction;
    }

    @Override
    public Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get, int completionId) {
        checkWANReplicationQueues();
        return delegate.createPutOperation(key, value, policy, get, completionId);
    }

    @Override
    public Operation createPutAllOperation(List<Map.Entry<Data, Data>> entries, ExpiryPolicy policy, int completionId) {
        checkWANReplicationQueues();
        return delegate.createPutAllOperation(entries, policy, completionId);
    }

    @Override
    public Operation createGetOperation(Data key, ExpiryPolicy policy) {
        return delegate.createGetOperation(key, policy);
    }

    @Override
    public Operation createContainsKeyOperation(Data key) {
        return delegate.createContainsKeyOperation(key);
    }

    @Override
    public Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        checkWANReplicationQueues();
        return delegate.createPutIfAbsentOperation(key, value, policy, completionId);
    }

    @Override
    public Operation createRemoveOperation(Data key, Data value, int completionId) {
        checkWANReplicationQueues();
        return delegate.createRemoveOperation(key, value, completionId);
    }

    @Override
    public Operation createGetAndRemoveOperation(Data key, int completionId) {
        checkWANReplicationQueues();
        return delegate.createGetAndRemoveOperation(key, completionId);
    }

    @Override
    public Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy, int completionId) {
        checkWANReplicationQueues();
        return delegate.createReplaceOperation(key, oldValue, newValue, policy, completionId);
    }

    @Override
    public Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        checkWANReplicationQueues();
        return delegate.createGetAndReplaceOperation(key, value, policy, completionId);
    }

    @Override
    public Operation createEntryProcessorOperation(Data key, Integer completionId,
                                                   EntryProcessor entryProcessor, Object... args) {
        checkWANReplicationQueues();
        return delegate.createEntryProcessorOperation(key, completionId, entryProcessor, args);
    }

    @Override
    public Operation createKeyIteratorOperation(int lastTableIndex, int fetchSize) {
        return delegate.createKeyIteratorOperation(lastTableIndex, fetchSize);
    }

    @Override
    public Operation createSetExpiryPolicyOperation(List<Data> keys, Data expiryPolicy) {
        return delegate.createSetExpiryPolicyOperation(keys, expiryPolicy);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy) {
        return delegate.createGetAllOperationFactory(keySet, policy);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues) {
        return delegate.createLoadAllOperationFactory(keySet, replaceExistingValues);
    }

    @Override
    public OperationFactory createClearOperationFactory() {
        return delegate.createClearOperationFactory();
    }

    @Override
    public OperationFactory createRemoveAllOperationFactory(Set<Data> keySet, Integer completionId) {
        return delegate.createRemoveAllOperationFactory(keySet, completionId);
    }

    @Override
    public OperationFactory createSizeOperationFactory() {
        return delegate.createSizeOperationFactory();
    }

    @Override
    public Operation createWanRemoveOperation(String origin, Data key, int completionId) {
        return delegate.createWanRemoveOperation(origin, key, completionId);
    }

    @Override
    public Operation createWanMergeOperation(String origin, CacheMergeTypes mergingEntry,
                                             SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy, int completionId) {
        return delegate.createWanMergeOperation(origin, mergingEntry, mergePolicy, completionId);
    }

    private void checkWANReplicationQueues() {
        WanReplicationPublisher publisher = publisherLookupFunction.apply(nameWithPrefix);
        publisher.checkWanReplicationQueues();
    }
}
