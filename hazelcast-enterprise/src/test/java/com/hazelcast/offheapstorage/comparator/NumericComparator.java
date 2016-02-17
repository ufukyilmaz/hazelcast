package com.hazelcast.offheapstorage.comparator;

import com.hazelcast.elastic.offheapstorage.OffHeapComparator;
import com.hazelcast.internal.memory.MemoryAccessor;

public class NumericComparator implements OffHeapComparator {

    private final MemoryAccessor mem;

    public NumericComparator(MemoryAccessor mem) {
        this.mem = mem;
    }

    @Override
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        long minLength = leftSize <= rightSize ? leftSize : (int) rightSize;

        for (long i = 0; i < minLength; i++) {
            byte rightByte = this.mem.getByte(rightAddress + i);
            byte leftByte = this.mem.getByte(leftAddress + i);

            byte leftBit = (byte) (leftByte & (byte) 0x80);
            byte rightBit = (byte) (rightByte & (byte) 0x80);

            if ((leftBit != 0) && (rightBit == 0)) {
                return 1;
            } else if ((leftBit == 0) && (rightBit != 0)) {
                return -1;
            }

            byte unsignedLeftBit = (byte) (leftByte & (byte) 0x7F);
            byte unsignedRightBit = (byte) (rightByte & (byte) 0x7F);

            if (unsignedLeftBit > unsignedRightBit) {
                return 1;
            } else if (leftByte < rightByte) {
                return -1;
            }
        }

        if (rightSize == leftSize) {
            return 0;
        }

        return (leftSize > rightSize) ? 1 : -1;
    }
}
