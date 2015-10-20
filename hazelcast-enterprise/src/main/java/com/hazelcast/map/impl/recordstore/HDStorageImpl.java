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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.SizeEstimators.createMapSizeEstimator;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * HiDensity backed {@code Storage} impl. for {@link com.hazelcast.core.IMap}.
 * This implementation can be used under multi-thread access.
 */
public class HDStorageImpl implements Storage<Data, HDRecord> {

    /**
     * Default capacity for a hash container.
     */
    private static final int DEFAULT_CAPACITY = 1000;

    /**
     * Default load factor.
     */
    private static final float DEFAULT_LOAD_FACTOR = 0.91f;

    private final BinaryElasticHashMap<HDRecord> map;
    private final HiDensityRecordProcessor recordProcessor;
    private final SizeEstimator sizeEstimator;

    public HDStorageImpl(HiDensityRecordProcessor<HDRecord> recordProcessor) {
        this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, recordProcessor);
    }

    HDStorageImpl(int initialCapacity, float loadFactor, HiDensityRecordProcessor<HDRecord> recordProcessor) {
        this.recordProcessor = recordProcessor;
        this.sizeEstimator = createMapSizeEstimator(NATIVE);
        this.map = new BinaryElasticHashMap<HDRecord>(initialCapacity, loadFactor, recordProcessor);
    }

    public HiDensityRecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    @Override
    public Object removeRecord(HDRecord record) {
        if (record == null) {
            return null;
        }

        Data key = record.getKey();
        HDRecord oldRecord = map.remove(key);

        addDeferredDispose(key);
        addDeferredDispose(oldRecord);
        return oldRecord == null ? null : oldRecord.getValue();
    }

    @Override
    public boolean containsKey(Data key) {
        return map.containsKey(key);
    }

    @Override
    public void put(Data key, HDRecord record) {
        HDRecord oldRecord = null;
        Data nativeKey = null;
        boolean succeed = false;
        try {
            nativeKey = toNative(key);
            record.setKeyAddress(((NativeMemoryData) nativeKey).address());
            oldRecord = map.put(nativeKey, record);
            succeed = true;
        } finally {
            if (succeed) {
                addDeferredDispose(oldRecord);
            } else {
                addDeferredDispose(record);
                addDeferredDispose(nativeKey);
            }
        }
    }

    // TODO remove unneeded ignored param.
    @Override
    public void updateRecordValue(Data ignored, HDRecord record, Object value) {
        Data oldValue = null;
        Data newValue = null;
        boolean succeed = false;
        try {
            oldValue = record.getValue();
            newValue = toNative(value);
            record.setValueAddress(((NativeMemoryData) newValue).address());
            succeed = true;
        } finally {
            if (succeed) {
                addDeferredDispose(oldValue);
            } else {
                addDeferredDispose(newValue);
            }
        }
    }

    @Override
    public HDRecord get(Data key) {
        return map.get(key);
    }

    @Override
    public void clear() {
        MemoryManager memoryManager = ((DefaultHiDensityRecordProcessor) recordProcessor).getMemoryManager();
        if (memoryManager == null || memoryManager.isDestroyed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        map.clear();
    }

    @Override
    public Collection<HDRecord> values() {
        return map.values();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void destroy() {

        MemoryManager memoryManager = ((DefaultHiDensityRecordProcessor) recordProcessor).getMemoryManager();
        if (memoryManager == null || memoryManager.isDestroyed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        dispose();
        map.destroy();
    }

    @Override
    public SizeEstimator getSizeEstimator() {
        return sizeEstimator;
    }

    @Override
    public void setSizeEstimator(SizeEstimator sizeEstimator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dispose() {
        recordProcessor.disposeDeferredBlocks();
    }

    private void addDeferredDispose(Object memoryBlock) {
        if (memoryBlock == null
                || ((MemoryBlock) memoryBlock).address() == NULL_ADDRESS
                || memoryBlock instanceof HeapData
                || !(memoryBlock instanceof MemoryBlock)) {
            return;
        }

        recordProcessor.addDeferredDispose(((MemoryBlock) memoryBlock));
    }

    private Data toNative(Data key) {
        return recordProcessor.convertData(key, DataType.NATIVE);
    }

    private Data toNative(Object value) {
        return recordProcessor.toData(value, DataType.NATIVE);
    }

}
