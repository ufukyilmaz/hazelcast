package com.hazelcast.offheapstorage.comparator;

import com.hazelcast.elastic.binarystorage.BinaryComparator;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.nio.OffHeapBits;

public class StringComparator implements BinaryComparator {

    private final MemoryAccessor mem;

    public StringComparator(MemoryAccessor mem) {
        this.mem = mem;
    }

    @Override
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        int leftLength = OffHeapBits.readInt(leftAddress, 1, true);
        int rightLength = OffHeapBits.readInt(rightAddress, 1, true);

        long minLength = leftLength <= rightLength ? leftLength : rightLength;

        long leftStart = 5 + leftAddress;
        long rightStart = 5 + rightAddress;

        for (long i = 0; i < minLength; i++) {
            int leftChar = mem.getByte(leftStart + i) & 0xff;
            int rightChar = mem.getByte(rightStart + i) & 0xff;

            if (leftChar > rightChar) {
                return 1;
            } else if (leftChar < rightChar) {
                return -1;
            }
        }

        if (leftLength == rightLength) {
            return 0;
        }

        return (leftLength > rightLength) ? 1 : -1;
    }
}

