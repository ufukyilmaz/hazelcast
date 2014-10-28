/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Set;

/**
 * TODO add a proper JavaDoc
 */
public class HiDensityOperationProvider implements CacheOperationProvider {

    private final String nameWithPrefix;

    public HiDensityOperationProvider(String nameWithPrefix) {
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
        return new CacheEntryProcessorOperation(nameWithPrefix, key, completionId, entryProcessor, args);
    }

    @Override
    public Operation createKeyIteratorOperation(int lastTableIndex, int fetchSize) {
        return new CacheKeyIteratorOperation(nameWithPrefix, lastTableIndex, fetchSize);
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
    public OperationFactory createClearOperationFactory(Set<Data> keySet, boolean isRemoveAll, Integer completionId) {
        return new CacheClearOperationFactory(nameWithPrefix, keySet, isRemoveAll, completionId);
    }

    @Override
    public OperationFactory createSizeOperationFactory() {
        return new CacheSizeOperationFactory(nameWithPrefix);
    }
}
