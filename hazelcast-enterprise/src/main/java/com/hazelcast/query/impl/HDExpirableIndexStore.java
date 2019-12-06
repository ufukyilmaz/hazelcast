package com.hazelcast.query.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.internal.serialization.Data;

/**
 * Expirable index store for HD memory.
 */
abstract class HDExpirableIndexStore extends BaseIndexStore {

    private final StoreAdapter partitionStoreAdapter;

    HDExpirableIndexStore(IndexCopyBehavior copyOn, StoreAdapter partitionStoreAdapter) {
        super(copyOn);
        this.partitionStoreAdapter = partitionStoreAdapter;
    }

    /**
     * Checks if the memoryBlock is expired and evicts it if so.
     * <p>
     * The memoryBlock is supposed to be a record or directly a value represented as NativeMemoryData.
     *
     * @param dataKey     data key
     * @param memoryBlock the memory block to be checked
     * @return an actual value (represented as NativeMemoryBlock) if the memoryBlock is not expired,
     * otherwise {@code null}.
     */
    NativeMemoryData getValueOrNullIfExpired(Data dataKey, MemoryBlock memoryBlock) {
        if (memoryBlock instanceof HDRecord) {
            HDRecord record = (HDRecord) memoryBlock;
            long now = Clock.currentTimeMillis();
            if (partitionStoreAdapter.evictIfExpired(dataKey, record, now, false)) {
                return null;
            } else {
                return (NativeMemoryData) record.getValue();
            }
        } else if (memoryBlock instanceof NativeMemoryData || memoryBlock == null) {
            return (NativeMemoryData) memoryBlock;
        } else {
            throw new IllegalStateException("Unexpected MemoryBlock given");
        }
    }

    /**
     * Gets a memory block to be stored in the index.
     *
     * @param entry the queryable entry
     * @return the HDRecord associated with the entry if ttl/maxIdle defined, otherwise the record's value.
     */
    MemoryBlock getValueToStore(QueryableEntry entry) {
        HDRecord record = (HDRecord) entry.getRecord();
        if (!partitionStoreAdapter.isExpirable() || !partitionStoreAdapter.isTtlOrMaxIdleDefined(record)) {
            return (NativeMemoryData) entry.getValueData();
        } else {
            return record;
        }
    }


    static NativeMemoryData getValueData(MemoryBlock value) {
        if (value == null) {
            return null;
        } else if (value instanceof HDRecord) {
            HDRecord record = (HDRecord) value;
            return (NativeMemoryData) record.getValue();
        } else if (value instanceof NativeMemoryData) {
            return (NativeMemoryData) value;
        } else {
            throw new IllegalStateException("Unexpected MemoryBlock given");
        }
    }
}
