package com.hazelcast.internal.hidensity;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.MemoryBlock;

/**
 * Base class of all record implementations for Hi-Density storage.
 *
 * @see com.hazelcast.internal.memory.MemoryBlock
 * @see NativeMemoryData
 */
public abstract class HiDensityRecord extends MemoryBlock {

    protected HiDensityRecord(GlobalMemoryAccessor memoryAccessor) {
        super(memoryAccessor);
    }

    protected HiDensityRecord(GlobalMemoryAccessor memoryAccessor, long address, int size) {
        super(memoryAccessor, address, size);
    }

    /**
     * Gets the address of value for this {@link HiDensityRecord}.
     *
     * @return the address of value for this {@link HiDensityRecord}.
     */
    public abstract long getValueAddress();

    /**
     * Sets the address of value for this {@link HiDensityRecord}.
     *
     * @param valueAddress the address of value for this {@link HiDensityRecord}
     */
    public abstract void setValueAddress(long valueAddress);

    /**
     * Resets the address of this {@link HiDensityRecord} to specified address
     * and this {@link HiDensityRecord} shows that allocated record area.
     *
     * @param address the address of allocated record area for this {@link HiDensityRecord}
     * @return the reset {@link HiDensityRecord} instance
     */
    public abstract HiDensityRecord reset(long address);

    /**
     * Clears value (address), creation times, access times, etc. of this {@link HiDensityRecord}.
     */
    public abstract void clear();

    /**
     * Returns sequence ID associated with this record.
     */
    public abstract long getSequence();
}
