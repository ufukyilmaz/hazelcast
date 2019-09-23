package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;

/**
 * Memory block accessor for HD index. It supports {@code NativeMemoryData} and {@code HDRecord} as a value.
 * The {@code HDRecord} is expected only when ttl/maxIdle is defined on the record.
 */
public class HDIndexBehmMemoryBlockAccessor implements MemoryBlockAccessor<MemoryBlock> {

    /**
     * If the highest bit ({@link HDRecord} bit) set in the address, it points to the {@link HDRecord},
     * otherwise {@link NativeMemoryData}.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    static final long HD_RECORD_BIT_MASK = 1L << 63;

    /**
     * The mask clears the {@link HDRecord} bit.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    static final long CLEAR_HD_RECORD_BIT_MASK = ~(1L << 63);

    private final MemoryBlockAccessor valueAccessor;
    private final MemoryBlockAccessor recordAccessor;

    public HDIndexBehmMemoryBlockAccessor(MemoryBlockAccessor valueAccessor, MemoryBlockAccessor recordAccessor) {
        this.valueAccessor = valueAccessor;
        this.recordAccessor = recordAccessor;
    }

    @Override
    public boolean isEqual(long address, MemoryBlock value) {
        // comparing different type of MemoryBlock(s)
        if (valueAddress(address) && !(value instanceof NativeMemoryData)
            || !valueAddress(address) && !(value instanceof HDRecord)) {
           return false;
        }
        return valueAddress(address) ? valueAccessor.isEqual(address, value)
                : recordAccessor.isEqual(pureAddress(address), value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        // comparing different type of MemoryBlock(s)
        if (((address1 ^ address2) & HD_RECORD_BIT_MASK) != 0) {
            return false;
        }
        return valueAddress(address1) ? valueAccessor.isEqual(address1, address2)
                : recordAccessor.isEqual(pureAddress(address1), pureAddress(address2));
    }

    @Override
    public MemoryBlock read(long address) {
        return valueAddress(address) ? valueAccessor.read(address) : recordAccessor.read(pureAddress(address));
    }

    @Override
    public long dispose(MemoryBlock block) {
        if (block instanceof NativeMemoryData) {
            return valueAccessor.dispose(block);
        } else if (block instanceof HDRecord) {
            return recordAccessor.dispose(block);
        } else {
            throw new IllegalStateException("Unexpected MemoryBlock given");
        }
    }

    @Override
    public long dispose(long address) {
        return valueAddress(address) ? valueAccessor.dispose(address) : recordAccessor.dispose(pureAddress(address));
    }

    private static boolean valueAddress(long address) {
        return (address & HD_RECORD_BIT_MASK) == 0;
    }

    private static long pureAddress(long address) {
        return address & CLEAR_HD_RECORD_BIT_MASK;
    }
}
