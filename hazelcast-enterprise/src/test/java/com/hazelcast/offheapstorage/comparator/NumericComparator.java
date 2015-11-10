package com.hazelcast.offheapstorage.comparator;

import sun.misc.Unsafe;
import com.hazelcast.elastic.offheapstorage.OffHeapComparator;

public class NumericComparator implements OffHeapComparator {
    private final Unsafe unsafe;

    public NumericComparator(Unsafe unsafe) {
        this.unsafe = unsafe;
    }

    @Override
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
