package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.util.HashUtil;

import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

public final class NativeMemoryDataUtil {

    private static final boolean BIG_ENDIAN = ByteOrder.BIG_ENDIAN == ByteOrder.nativeOrder();

    private NativeMemoryDataUtil() {
    }

    public static boolean equals(long address, Data data) {
        if (data instanceof NativeMemoryData) {
            return equals(address, ((NativeMemoryData) data).address());
        }

        if (address == NULL_ADDRESS) {
            return false;
        }

        int type = readType(address);
        if (type != data.getType()) {
            return false;
        }

        int bufferSize = readDataSize(address);
        if (bufferSize != data.dataSize()) {
            return false;
        }

        if (bufferSize == 0) {
            return true;
        }
        return equals(address, bufferSize, data.toByteArray());
    }

    static int readType(long address) {
        if (address == 0L) {
            return SerializationConstants.CONSTANT_TYPE_NULL;
        }
        int type = MEM.getInt(address + NativeMemoryData.TYPE_OFFSET);
        return BIG_ENDIAN ? type : Integer.reverseBytes(type);
    }

    static int readDataSize(long address) {
        return Math.max(readTotalSize(address) - HeapData.HEAP_DATA_OVERHEAD, 0);
    }

    static int readTotalSize(long address) {
        if (address == 0L) {
            return 0;
        }
        return MEM.getInt(address + NativeMemoryData.SIZE_OFFSET);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public static boolean equals(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        if (readType(address1) != readType(address2)) {
            return false;
        }
        int bufferSize1 = readDataSize(address1);
        int bufferSize2 = readDataSize(address2);
        if (bufferSize1 != bufferSize2) {
            return false;
        }
        if (bufferSize1 == 0) {
            return true;
        }
        return equals(address1, address2, bufferSize1);
    }

    public static boolean equals(long address1, long address2, int bufferSize) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        int numLongs = bufferSize / LONG_SIZE_IN_BYTES;
        int remaining = bufferSize % LONG_SIZE_IN_BYTES;
        final int lastAddress = NativeMemoryData.DATA_OFFSET + bufferSize - 1;
        for (int i = 0; i < remaining; i++) {
            byte k1 = MEM.getByte(address1 + lastAddress - i);
            byte k2 = MEM.getByte(address2 + lastAddress - i);
            if (k1 != k2) {
                return false;
            }
        }
        for (int i = 0; i < numLongs; i++) {
            long k1 = MEM.getLong(address1 + NativeMemoryData.DATA_OFFSET + (i * LONG_SIZE_IN_BYTES));
            long k2 = MEM.getLong(address2 + NativeMemoryData.DATA_OFFSET + (i * LONG_SIZE_IN_BYTES));
            if (k1 != k2) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(long address, final int bufferSize, byte[] bytes) {
        if (address == NULL_ADDRESS || bytes == null || bytes.length == 0
                || bufferSize != bytes.length - HeapData.HEAP_DATA_OVERHEAD) {
            return false;
        }
        int bufferOffset = NativeMemoryData.DATA_OFFSET;
        for (int i = 0; i < bufferSize; i++) {
            byte b = MEM.getByte(address + bufferOffset + i);
            if (b != bytes[i + HeapData.DATA_OFFSET]) {
                return false;
            }
        }
        return true;
    }

    public static int hashCode(long address) {
        int bufferSize = readDataSize(address);
        return HashUtil.MurmurHash3_x86_32_direct(address, NativeMemoryData.DATA_OFFSET, bufferSize);
    }

    public static long hash64(long address) {
        int bufferSize = readDataSize(address);
        return HashUtil.MurmurHash3_x64_64_direct(address, NativeMemoryData.DATA_OFFSET, bufferSize);
    }

    public static void dispose(EnterpriseSerializationService ess, MemoryAllocator malloc, MemoryBlock... blocks) {
        NativeMemoryData[] nativeMemoryData = new NativeMemoryData[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            MemoryBlock block = blocks[i];
            nativeMemoryData[i] = new NativeMemoryData(block.address(), block.size());
        }

        dispose(ess, malloc, nativeMemoryData);
    }

    // Avoids having TCFTC anti-pattern.
    public static void dispose(EnterpriseSerializationService ess, MemoryAllocator malloc, NativeMemoryData... nativeData) {
        Exception caught = null;
        try {
            for (NativeMemoryData data : nativeData) {
                try {
                    if (data != null) {
                        ess.disposeData(data, malloc);
                    }
                } catch (Exception ex) {
                    // best effort, try to deallocate all native data, throw last exception;
                    caught = ex;
                }
            }
        } finally {
            if (caught != null) {
                throw new HazelcastException("Could not deallocate native data. There may be a native memory leak!", caught);
            }
        }
    }
}
