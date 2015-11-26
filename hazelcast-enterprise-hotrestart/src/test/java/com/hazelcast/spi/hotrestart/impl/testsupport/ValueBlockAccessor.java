package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;

import java.nio.ByteBuffer;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;
import static com.hazelcast.spi.hotrestart.impl.testsupport.Long2bytesMap.TOMBSTONE_SEQ_SIZE;

public class ValueBlockAccessor extends MemoryBlock {
    public static final int HEADER_SIZE = 4;
    private final MemoryAllocator malloc;
    private boolean isTombstone;
    private int valueSize;

    ValueBlockAccessor(MemoryAllocator malloc) {
        setSize(HEADER_SIZE);
        this.malloc = malloc;
    }

    final void reset(long vSlotAddr) {
        assert vSlotAddr > NULL_ADDRESS : "Attempt to reset to invalid address: " + vSlotAddr;
        final long address = UNSAFE.getLong(vSlotAddr);
        assert address > NULL_ADDRESS : "Read an invalid address from value slot: " + address;
        setAddress(address);
        final int sizeFromHeader = readInt(0);
        updateLocalState(sizeFromHeader);
    }

    final boolean isTombstone() {
        return isTombstone;
    }

    final long tombstoneSeq() {
        return readLong(HEADER_SIZE);
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

    final void allocateTombstone(long tombstoneSeq) {
        final int size = HEADER_SIZE + TOMBSTONE_SEQ_SIZE;
        final long addr = malloc.allocate(size);
        resetToNew(addr, -TOMBSTONE_SEQ_SIZE);
        writeLong(HEADER_SIZE, tombstoneSeq);
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
        if (sizeInHeader == -TOMBSTONE_SEQ_SIZE) {
            this.isTombstone = true;
            this.valueSize = 0;
            setSize(HEADER_SIZE + TOMBSTONE_SEQ_SIZE);
        } else {
            this.isTombstone = false;
            this.valueSize = sizeInHeader;
            setSize(HEADER_SIZE + sizeInHeader);
        }
    }
}
