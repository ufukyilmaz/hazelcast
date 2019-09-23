package com.hazelcast.internal.elastic.tree;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.nio.Bits;
import org.junit.Before;

import java.util.Iterator;


public abstract class OffHeapTreeTestSupport {

    MemoryAllocator malloc;

    @Before
    public void initMalloc()
            throws Exception {
        malloc = new StandardMemoryManager(MemorySize.parse("100M"));
    }

    MemoryBlock createBlob(int value) {
        MemoryBlock key = new MemoryBlock(
                malloc.allocate(Bits.INT_SIZE_IN_BYTES),
                Bits.INT_SIZE_IN_BYTES);
        key.writeInt(0, value);
        return key;
    }

    int entryCount(Iterator<OffHeapTreeEntry> entries) {
        int count = 0;
        while (entries.hasNext()) {
            entries.next();
            count++;
        }

        return count;
    }

    int valueCount(Iterator<OffHeapTreeEntry> entries) {
        int count = 0;
        while (entries.hasNext()) {
            Iterator<MemoryBlock> values = entries.next().values();
            while (values.hasNext()) {
                values.next();
                count++;
            }
        }

        return count;
    }

    public static class DataComparator implements OffHeapComparator {

        private final GlobalMemoryAccessor unsafe;

        DataComparator(GlobalMemoryAccessor unsafe) {
            this.unsafe = unsafe;
        }

        @Override
        @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:magicnumber"})
        public int compare(MemoryBlock lBlob, MemoryBlock rBlob) {
            if (lBlob.address() == rBlob.address()) {
                return 0;
            }

            long minLength = lBlob.size() <= rBlob.size() ? lBlob.size() : rBlob.size();

            for (long i = 0; i < minLength; i++) {
                int result = compareByte(
                        this.unsafe.getByte(lBlob.address() + i),
                        this.unsafe.getByte(rBlob.address() + i));

                if (result != 0) {
                    return result;
                }
            }

            if (rBlob.size() == lBlob.size()) {
                return 0;
            }

            return (lBlob.size() > rBlob.size()) ? 1 : -1;
        }

        @Override
        public int compare(byte[] lBlob, byte[] rBlob) {
            long minLength = lBlob.length <= rBlob.length ? lBlob.length : rBlob.length;

            for (int i = 0; i < minLength; i++) {
                int result = compareByte(lBlob[i], rBlob[i]);

                if (result != 0) {
                    return result;
                }
            }

            if (rBlob.length == lBlob.length) {
                return 0;
            }

            return (lBlob.length > rBlob.length) ? 1 : -1;
        }

        private int compareByte(byte left, byte right) {
            byte leftBit = (byte) (left & (byte) 0x80);
            byte rightBit = (byte) (right & (byte) 0x80);

            if ((leftBit != 0) && (rightBit == 0)) {
                return 1;
            } else if ((leftBit == 0) && (rightBit != 0)) {
                return -1;
            }

            byte unsignedLeftBit = (byte) (left & (byte) 0x7F);
            byte unsignedRightBit = (byte) (right & (byte) 0x7F);

            if (unsignedLeftBit > unsignedRightBit) {
                return 1;
            } else if (left < right) {
                return -1;
            }

            return 0;
        }
    }

}
