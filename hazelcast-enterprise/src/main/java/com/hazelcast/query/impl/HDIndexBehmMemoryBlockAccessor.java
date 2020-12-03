package com.hazelcast.query.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Memory block accessor for HD index. It
 * supports {@code NativeMemoryData}  as a value.
 */
public class HDIndexBehmMemoryBlockAccessor implements MemoryBlockAccessor<MemoryBlock> {

    private final MemoryBlockAccessor valueAccessor;

    public HDIndexBehmMemoryBlockAccessor(MemoryBlockAccessor valueAccessor) {
        this.valueAccessor = valueAccessor;
    }

    @Override
    public boolean isEqual(long address, MemoryBlock value) {
        return valueAccessor.isEqual(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return valueAccessor.isEqual(address1, address2);

    }

    @Override
    public MemoryBlock read(long address) {
        return valueAccessor.read(address);
    }

    @Override
    public long dispose(MemoryBlock block) {
        if (block instanceof NativeMemoryData) {
            return valueAccessor.dispose(block);
        } else {
            throw new IllegalStateException("Unexpected MemoryBlock given");
        }
    }

    @Override
    public long dispose(long address) {
        return valueAccessor.dispose(address);
    }
}
