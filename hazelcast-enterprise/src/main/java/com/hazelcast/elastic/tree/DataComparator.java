package com.hazelcast.elastic.tree;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;

/**
 * Comparator of the off-heap Data values given to the compare methods as native addresses and sizes.
 * Will NOT copy the data to the heap and will NOT deserialize the objects
 */
public class DataComparator implements OffHeapComparator {

    private final GlobalMemoryAccessor unsafe;

    public DataComparator(GlobalMemoryAccessor unsafe) {
        this.unsafe = unsafe;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:magicnumber"})
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        long minLength = leftSize <= rightSize ? leftSize : (int) rightSize;

        for (long i = 0; i < minLength; i++) {
            byte rightByte = this.unsafe.getByte(rightAddress + i);
            byte leftByte = this.unsafe.getByte(leftAddress + i);

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
