package com.hazelcast.elastic.tree;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.memory.MemoryBlock;

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
    public int compare(MemoryBlock lBlob, MemoryBlock rBlob) {
        long minLength = lBlob.size() <= rBlob.size() ? lBlob.size() : rBlob.size();

        for (long i = 0; i < minLength; i++) {
            byte rightByte = this.unsafe.getByte(rBlob.address() + i);
            byte leftByte = this.unsafe.getByte(lBlob.address() + i);

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

        if (rBlob.size() == lBlob.size()) {
            return 0;
        }

        return (lBlob.size() > rBlob.size()) ? 1 : -1;
    }

}
