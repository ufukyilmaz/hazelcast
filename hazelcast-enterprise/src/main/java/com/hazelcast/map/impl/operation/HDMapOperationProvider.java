package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.query.HDQueryOperation;
import com.hazelcast.map.impl.query.HDQueryPartitionOperation;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.tx.HDTxnDeleteOperation;
import com.hazelcast.map.impl.tx.HDTxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.HDTxnSetOperation;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * Provides {@link com.hazelcast.core.IMap IMap} operations when
 * {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} is
 * {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE}.
 */
@SuppressWarnings({
        "checkstyle:methodcount",
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity"
})
public class HDMapOperationProvider implements MapOperationProvider {

    @Override
    public OperationFactory createMapSizeOperationFactory(String name) {
        return new HDSizeOperationFactory(name);
    }

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return new HDPutOperation(name, key, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        return new HDTryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        return new HDSetOperation(name, dataKey, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return new HDPutIfAbsentOperation(name, key, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return new HDPutTransientOperation(name, key, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key, boolean disableWanReplicationEvent) {
        return new HDRemoveOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        return new HDTryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createReplaceOperation(String name, Data dataKey, Data value) {
        return new HDReplaceOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value) {
        return new HDRemoveIfSameOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        return new HDReplaceIfSameOperation(name, dataKey, expect, update);
    }

    @Override
    public MapOperation createDeleteOperation(String name, Data key, boolean disableWanReplicationEvent) {
        return new HDDeleteOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createClearOperation(String name) {
        return new HDClearOperation(name);
    }

    @Override
    public MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        return new HDEntryOperation(name, dataKey, entryProcessor);
    }

    @Override
    public MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        return new HDEvictOperation(name, dataKey, asyncBackup);
    }

    @Override
    public MapOperation createEvictAllOperation(String name) {
        return new HDEvictAllOperation(name);
    }

    @Override
    public MapOperation createSetTtlOperation(String name, Data dataKey, long ttl) {
        return new HDSetTtlOperation(name, dataKey, ttl);
    }

    @Override
    public MapOperation createContainsKeyOperation(String name, Data dataKey) {
        return new HDContainsKeyOperation(name, dataKey);
    }

    @Override
    public OperationFactory createContainsValueOperationFactory(String name, Data testValue) {
        return new HDContainsValueOperationFactory(name, testValue);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(String name, List<Data> keys) {
        return new HDMapGetAllOperationFactory(name, keys);
    }

    @Override
    public OperationFactory createEvictAllOperationFactory(String name) {
        return new HDEvictAllOperationFactory(name);
    }

    @Override
    public OperationFactory createClearOperationFactory(String name) {
        return new HDClearOperationFactory(name);
    }

    @Override
    public OperationFactory createMapFlushOperationFactory(String name) {
        return new HDMapFlushOperationFactory(name);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(String name, List<Data> keys, boolean replaceExistingValues) {
        return new HDMapLoadAllOperationFactory(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createTxnDeleteOperation(String name, Data dataKey, long version) {
        return new HDTxnDeleteOperation(name, dataKey, version);
    }

    @Override
    public MapOperation createTxnLockAndGetOperation(
            String name, Data dataKey, long timeout, long ttl, String ownerUuid, boolean shouldLoad, boolean blockReads) {
        return new HDTxnLockAndGetOperation(name, dataKey, timeout, ttl, ownerUuid, shouldLoad, blockReads);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        return new HDTxnSetOperation(name, dataKey, value, version, ttl);
    }

    @Override
    public MapOperation createLegacyMergeOperation(String name, EntryView<Data, Data> entryView,
                                                   MapMergePolicy policy, boolean disableWanReplicationEvent) {
        return new HDLegacyMergeOperation(name, entryView, policy, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createMergeOperation(String name, MapMergeTypes mergingEntry,
                                             SplitBrainMergePolicy<Data, MapMergeTypes> policy,
                                             boolean disableWanReplicationEvent) {
        return new HDMergeOperation(name, singletonList(mergingEntry), policy, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createMapFlushOperation(String name) {
        return new HDMapFlushOperation(name);
    }

    @Override
    public MapOperation createLoadMapOperation(String name, boolean replaceExistingValues) {
        return new HDLoadMapOperation(name, replaceExistingValues);
    }

    @Override
    public MapOperation createFetchKeysOperation(String name, int lastTableIndex, int fetchSize) {
        return new HDMapFetchKeysOperation(name, lastTableIndex, fetchSize);
    }

    @Override
    public MapOperation createFetchEntriesOperation(String name, int lastTableIndex, int fetchSize) {
        return new HDMapFetchEntriesOperation(name, lastTableIndex, fetchSize);
    }

    @Override
    public MapOperation createFetchWithQueryOperation(String name, int lastTableIndex, int fetchSize, Query query) {
        return new HDMapFetchWithQueryOperation(name, lastTableIndex, fetchSize, query);
    }

    @Override
    public MapOperation createGetEntryViewOperation(String name, Data dataKey) {
        return new HDGetEntryViewOperation(name, dataKey);
    }

    @Override
    public OperationFactory createPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        return new HDPartitionWideEntryOperationFactory(name, entryProcessor);
    }

    @Override
    public OperationFactory createPartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor,
                                                                                  Predicate predicate) {
        return new HDPartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
    }

    @Override
    public OperationFactory createMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        return new HDMultipleEntryOperationFactory(name, keys, entryProcessor);
    }

    @Override
    public MapOperation createGetOperation(String name, Data dataKey) {
        return new HDGetOperation(name, dataKey);
    }

    @Override
    public MapOperation createQueryOperation(Query query) {
        return new HDQueryOperation(query);
    }

    @Override
    public MapOperation createQueryPartitionOperation(Query query) {
        return new HDQueryPartitionOperation(query);
    }

    @Override
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        return new HDLoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public OperationFactory createPutAllOperationFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        return new HDPutAllPartitionAwareOperationFactory(name, partitions, mapEntries);
    }

    @Override
    public OperationFactory createMergeOperationFactory(String name, int[] partitions, List<MapMergeTypes>[] mergingEntries,
                                                        SplitBrainMergePolicy<Data, MapMergeTypes> policy) {
        return new HDMergeOperationFactory(name, partitions, mergingEntries, policy);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries) {
        return new HDPutAllOperation(name, mapEntries);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        return new HDPutFromLoadAllOperation(name, keyValueSequence);
    }
}