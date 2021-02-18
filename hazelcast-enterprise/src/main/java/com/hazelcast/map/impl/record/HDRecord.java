package com.hazelcast.map.impl.record;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;

/**
 * Base class to derive {@link HDRecord} implementations.
 */
public abstract class HDRecord extends HiDensityRecord implements Record<Data> {

    public HDRecord(GlobalMemoryAccessor memoryAccessor) {
        super(memoryAccessor);
    }

    public HDRecord(GlobalMemoryAccessor memoryAccessor, long address, int size) {
        super(memoryAccessor, address, size);
    }

    @Override
    public final void clear() {
        zero();
    }

    @Override
    public HDRecord reset(long address) {
        setAddress(address);
        setSize(size());
        return this;
    }

    @Override
    public long getSequence() {
        return UNSET;
    }

    @Override
    public long getCost() {
        // This is heap cost. For NATIVE
        // we are not calculating this cost
        return 0;
    }

    public final String toString() {
        String className = getClass().getSimpleName();
        return address() == NULL_PTR
                ? className + "{NULL}"
                : className + "{version: " + getVersion()
                + ", creationTime: " + getCreationTime()
                + ", lastAccessTime: " + getLastAccessTime()
                + ", lastUpdateTime: " + getLastUpdateTime()
                + ", lastStoredTime: " + getLastStoredTime()
                + ", hits: " + getHits()
                + ", sequence: " + getSequence()
                + ", valueAddress: " + getValueAddress()
                + " }";
    }
}
