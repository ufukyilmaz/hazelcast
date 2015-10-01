package com.hazelcast.hidensity.impl;

import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.elastic.map.SampleableElasticHashMap;
import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordMap;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;

/**
 * @author sozal 18/02/15
 *
 * @param <R> the type of the {@link HiDensityRecord} to be stored
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

    @Override
    public boolean set(Data key, R value) {
        boolean added = super.set(key, value);
        if (added) {
            storageInfo.increaseEntryCount();
        }
        return added;
    }

    @Override
    public R put(Data key, R value) {
        R record = super.put(key, value);
        // If there is no previous value with specified key, means that new entry is added
        if (record == null) {
            storageInfo.increaseEntryCount();
        }
        return record;
    }

    @Override
    public R putIfAbsent(Data key, R value) {
        R record = super.putIfAbsent(key, value);
        // If there is no previous value with specified key, means that new entry is added
        if (record == null) {
            storageInfo.increaseEntryCount();
        }
        return record;
    }

    @Override
    public boolean delete(Data key) {
        boolean deleted = super.delete(key);
        if (deleted) {
            storageInfo.decreaseEntryCount();
        }
        return deleted;
    }

    @Override
    public R remove(Object k) {
        R removedRecord = super.remove(k);
        if (removedRecord != null) {
            storageInfo.decreaseEntryCount();
        }
        return removedRecord;
    }

    @Override
    public boolean remove(Object k, Object v) {
        boolean removed = super.remove(k, v);
        if (removed) {
            storageInfo.decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public void clear() {
        final int sizeBeforeClear = size();
        super.clear();
        storageInfo.removeEntryCount(sizeBeforeClear);
    }

    protected NativeOutOfMemoryError onOome(NativeOutOfMemoryError e) {
        return new NativeOutOfMemoryError("Cannot expand internal cache map -> " + e.getMessage(), e);
    }

    /**
     * Returns an slottable iterator for this {@link HiDensityRecordMap} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    public SlottableIterator<Map.Entry<Data, R>> iterator(int slot) {
        return new EntryIter(slot);
    }

}
