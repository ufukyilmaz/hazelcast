/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.tx.HDTxnDeleteOperation;
import com.hazelcast.map.impl.tx.HDTxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.HDTxnSetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;

import java.util.List;
import java.util.Set;

/**
 * Provides {@link com.hazelcast.core.IMap IMap} operations when
 * {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} is
 * {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE}.
 */
public class HDMapOperationProvider implements MapOperationProvider {

    public HDMapOperationProvider() {
    }

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl) {
        return new HDPutOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        return new HDTryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl) {
        return new HDSetOperation(name, dataKey, value, ttl);
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl) {
        return new HDPutIfAbsentOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl) {
        return new HDPutTransientOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key) {
        return new HDRemoveOperation(name, key);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        return new HDTryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createWanOriginatedDeleteOperation(String name, Data key) {
        return new HDWanOriginatedDeleteOperation(name, key);
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
    public MapOperation createDeleteOperation(String name, Data key) {
        return new HDDeleteOperation(name, key);
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
    public MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, String ownerUuid) {
        return new HDTxnLockAndGetOperation(name, dataKey, timeout, ttl, ownerUuid);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        return new HDTxnSetOperation(name, dataKey, value, version, ttl);
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
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        return new HDLoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries, boolean initialLoad) {
        return new HDPutAllOperation(name, mapEntries, initialLoad);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        return new HDPutFromLoadAllOperation(name, keyValueSequence);
    }

}
