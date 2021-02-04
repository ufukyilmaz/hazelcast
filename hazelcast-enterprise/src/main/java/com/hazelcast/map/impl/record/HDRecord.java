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
    public int getVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setVersion(int version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastAccessTime() {
        return UNSET;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
    }

    @Override
    public long getLastUpdateTime() {
        return UNSET;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
    }

    @Override
    public long getCreationTime() {
        return UNSET;
    }

    @Override
    public void setCreationTime(long creationTime) {
    }

    @Override
    public int getHits() {
        return UNSET;
    }

    @Override
    public void setHits(int hits) {
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        return false;
    }

    @Override
    public long getSequence() {
        return UNSET;
    }

    @Override
    public void setSequence(long sequence) {
    }

    @Override
    public long getLastStoredTime() {
        return UNSET;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
    }

    @Override
    public long getCost() {
        // This is heap cost. For NATIVE
        // we are not calculating this cost
        return 0;
    }

    @Override
    public int getRawCreationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawCreationTime(int creationTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastAccessTime(int lastAccessTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastUpdateTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastUpdateTime(int lastUpdateTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastStoredTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastStoredTime(int time) {
        throw new UnsupportedOperationException();
    }

    @Override
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
