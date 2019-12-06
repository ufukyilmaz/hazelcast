package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.internal.elastic.SlottableIterator;
import com.hazelcast.internal.elastic.map.SampleableElasticHashMap;
import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.serialization.Data;

import java.util.Map;

/**
 * @param <R> type of the {@link HiDensityRecord} to be stored
 */
public class DefaultHiDensityRecordMap<R extends HiDensityRecord>
        extends SampleableElasticHashMap<R>
        implements HiDensityRecordMap<R> {

    protected final transient HiDensityRecordProcessor<R> recordProcessor;
    protected final transient HiDensityStorageInfo storageInfo;

    public DefaultHiDensityRecordMap(int initialCapacity,
                                     HiDensityRecordProcessor<R> recordProcessor,
                                     HiDensityStorageInfo storageInfo) {
        super(initialCapacity, recordProcessor);
        this.recordProcessor = recordProcessor;
        this.storageInfo = storageInfo;
    }

    protected void increaseEntryCount() {
        storageInfo.increaseEntryCount();
    }

    protected void decreaseEntryCount() {
        storageInfo.decreaseEntryCount();
    }

    protected void decreaseEntryCount(int entryCount) {
        storageInfo.removeEntryCount(entryCount);
    }

    @Override
    public boolean set(Data key, R value) {
        boolean added = super.set(key, value);
        if (added) {
            increaseEntryCount();
        }
        return added;
    }

    @Override
    public R put(Data key, R value) {
        R record = super.put(key, value);
        // if there is no previous value with specified key, means that new entry is added
        if (record == null) {
            increaseEntryCount();
        }
        return record;
    }

    @Override
    public R putIfAbsent(Data key, R value) {
        R record = super.putIfAbsent(key, value);
        // if there is no previous value with specified key, it means that new entry is added
        if (record == null) {
            increaseEntryCount();
        }
        return record;
    }

    @Override
    public boolean delete(Data key) {
        boolean deleted = super.delete(key);
        if (deleted) {
            decreaseEntryCount();
        }
        return deleted;
    }

    @Override
    public R remove(Object k) {
        R removedRecord = super.remove(k);
        if (removedRecord != null) {
            decreaseEntryCount();
        }
        return removedRecord;
    }

    @Override
    public boolean remove(Object k, Object v) {
        boolean removed = super.remove(k, v);
        if (removed) {
            decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public void clear() {
        final int sizeBeforeClear = size();
        super.clear();
        decreaseEntryCount(sizeBeforeClear);
    }

    @Override
    protected NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return new NativeOutOfMemoryError("Cannot expand internal cache map -> " + e.getMessage(), e);
    }

    /**
     * Returns a slottable iterator for this {@link HiDensityRecordMap} to iterate over records.
     *
     * @param slot the slot number (or index) to start the {@code iterator}
     * @return the slottable iterator for specified {@code slot}
     */
    public SlottableIterator<Map.Entry<Data, R>> iterator(int slot) {
        return entryIter(slot);
    }
}
