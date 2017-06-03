package com.hazelcast.elastic.tree;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.StandardMemoryManager;
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

}
