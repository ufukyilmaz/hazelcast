package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.spi.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;

import java.nio.ByteBuffer;

import static com.hazelcast.spi.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.spi.memory.MemoryAllocator.NULL_ADDRESS;

public class ValueBlockAccessor extends MemoryBlock {
    public static final int HEADER_SIZE = 4;
    private final MemoryAllocator malloc;
    private int valueSize;

    ValueBlockAccessor(MemoryAllocator malloc) {
        setSize(HEADER_SIZE);
        this.malloc = malloc;
    }

    final void reset(long vSlotAddr) {
        assert vSlotAddr > NULL_ADDRESS : "Attempt to reset to invalid address: " + vSlotAddr;
        final long address = AMEM.getLong(vSlotAddr);
        assert address > NULL_ADDRESS : "Read an invalid address from value slot: " + address;
        setAddress(address);
        final int sizeFromHeader = readInt(0);
        updateLocalState(sizeFromHeader);
    }

    final int valueSize() {
        return valueSize;
    }

    final void copyInto(ByteBuffer bb) {
        final int position = bb.position();
        final int length = valueSize();
        copyToByteArray(HEADER_SIZE, bb.array(), 0, length);
        bb.position(position + length);
    }

    final void allocate(byte[] bytes) {
        final int size = HEADER_SIZE + bytes.length;
        final long addr = malloc.allocate(size);
        resetToNew(addr, bytes.length);
        copyFromByteArray(HEADER_SIZE, bytes, 0, bytes.length);
    }

    final void delete() {
        malloc.free(address(), size());
        setAddress(NULL_ADDRESS);
    }

    private void resetToNew(long address, int sizeInHeader) {
        setAddress(address);
        setSize(HEADER_SIZE);
        writeInt(0, sizeInHeader);
        updateLocalState(sizeInHeader);
    }

    private void updateLocalState(int sizeInHeader) {
        this.valueSize = sizeInHeader;
        setSize(HEADER_SIZE + sizeInHeader);
    }
}
