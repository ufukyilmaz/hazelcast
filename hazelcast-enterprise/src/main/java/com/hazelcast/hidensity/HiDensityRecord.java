package com.hazelcast.hidensity;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Base class of all record implementations for Hi-Density storage
 *
 * @see com.hazelcast.memory.MemoryBlock
 * @see NativeMemoryData
 *
 * @author sozal 18/02/15
 */
public abstract class HiDensityRecord
        extends MemoryBlock {

    protected HiDensityRecord() {
    }

    protected HiDensityRecord(long address, int size) {
        super(address, size);
    }

    /**
     * Gets the address of value for this {@link HiDensityRecord}.
     *
     * @return the address of value for this {@link HiDensityRecord}
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
     * Clears value (address), creation times, access times and etc ... of this {@link HiDensityRecord}.
     */
    public abstract void clear();

}
