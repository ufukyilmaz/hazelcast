package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

class ValueBlockAccessor extends MemoryBlock {

    private static final int HEADER_SIZE = 4;

    private final MemoryAllocator malloc;
    private final MemoryAccessor mem;
    private int valueSize;

    ValueBlockAccessor(MemoryManager memMgr) {
        super((GlobalMemoryAccessor) memMgr.getAccessor());
        setSize(HEADER_SIZE);
        this.malloc = memMgr.getAllocator();
        this.mem = memMgr.getAccessor();
    }

    final void reset(long vSlotAddr) {
        assert vSlotAddr != NULL_ADDRESS : "Attempt to reset to invalid address: " + vSlotAddr;
        final long address = mem.getLong(vSlotAddr);
        assert address != NULL_ADDRESS : "Read an invalid address from value slot: " + address;
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
