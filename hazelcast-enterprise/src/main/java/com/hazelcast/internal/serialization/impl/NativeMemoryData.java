package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.util.HashUtil;

import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil.readDataSize;
import static com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil.readTotalSize;
import static com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil.readType;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * NativeMemoryData is in the form of {@code |SIZE|PARTITION HASH CODE|TYPE-ID|SERIALIZER DATA|}.
 */
public final class NativeMemoryData extends MemoryBlock implements Data {

    public static final int SIZE_OFFSET = 0;
    public static final int PARTITION_HASH_OFFSET = 4;
    public static final int TYPE_OFFSET = 8;
    public static final int DATA_OFFSET = 12;

    /**
     * Native memory data size overhead relative to wrapped data.
     */
    public static final int NATIVE_MEMORY_DATA_OVERHEAD = Bits.INT_SIZE_IN_BYTES;

    private static final boolean BIG_ENDIAN = ByteOrder.BIG_ENDIAN == ByteOrder.nativeOrder();

    public NativeMemoryData() {
    }

    public NativeMemoryData(long address, int size) {
        super(address, size);
    }

    @Override
    public int totalSize() {
        return readTotalSize(address());
    }

    @Override
    public int dataSize() {
        return readDataSize(address());
    }

    @Override
    public int getPartitionHash() {
        int hash = getPartitionHashCode();
        if (hash != 0) {
            return BIG_ENDIAN ? hash : Integer.reverseBytes(hash);
        }
        return hashCode();
    }

    private int getPartitionHashCode() {
        if (address == 0L) {
            return 0;
        }
        return readInt(PARTITION_HASH_OFFSET);
    }

    @Override
    public boolean hasPartitionHash() {
        int hash = getPartitionHashCode();
        return hash != 0;
    }

    @Override
    public byte[] toByteArray() {
        int len = totalSize();
        byte[] buffer = new byte[len];
        copyTo(NATIVE_MEMORY_DATA_OVERHEAD, buffer, ARRAY_BYTE_BASE_OFFSET, len);
        return buffer;
    }

    @Override
    public void copyTo(byte[] dest, int destPos) {
        copyTo(NATIVE_MEMORY_DATA_OVERHEAD, dest, ARRAY_BYTE_BASE_OFFSET + destPos, totalSize());
    }

    @Override
    public int getType() {
        return readType(address());
    }

    @Override
    public int getHeapCost() {
        return LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    }

    @Override
    public boolean isPortable() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE == getType();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!(o instanceof Data)) {
            return false;
        }

        Data data = (Data) o;
        if (getType() != data.getType()) {
            return false;
        }

        final int dataSize = dataSize();
        if (dataSize != data.dataSize()) {
            return false;
        }
        if (dataSize == 0) {
            return true;
        }
        if (data instanceof NativeMemoryData) {
            return NativeMemoryDataUtil.equals(address(), ((NativeMemoryData) data).address(), dataSize);
        }
        byte[] bytes = data.toByteArray();
        return NativeMemoryDataUtil.equals(address(), dataSize, bytes);
    }

    @Override
    public int hashCode() {
        if (address == 0L) {
            return 0;
        }
        return HashUtil.MurmurHash3_x86_32_direct(address(), DATA_OFFSET, dataSize());
    }

    @Override
    public long hash64() {
        if (address == 0L) {
            return 0L;
        }
        return HashUtil.MurmurHash3_x64_64_direct(address(), DATA_OFFSET, dataSize());
    }

    public NativeMemoryData reset(long address) {
        setAddress(address);
        if (address > 0L) {
            // tmp size to read data size;
            setSize(INT_SIZE_IN_BYTES);
            setSize(totalSize() + INT_SIZE_IN_BYTES);
        } else {
            setSize(0);
        }
        return this;
    }

    public NativeMemoryData reset(long address, int size) {
        setAddress(address);
        if (address > 0L) {
            setSize(size);
        }

        return this;
    }

    @Override
    public boolean isJson() {
        return SerializationConstants.JAVASCRIPT_JSON_SERIALIZATION_TYPE == getType();
    }

    @Override
    public String toString() {
        if (address() > 0L) {
            final StringBuilder sb = new StringBuilder("NativeMemoryData{");
            sb.append("type=").append(getType());
            sb.append(", hashCode=").append(hashCode());
            sb.append(", partitionHash=").append(getPartitionHash());
            sb.append(", totalSize=").append(totalSize());
            sb.append(", dataSize=").append(dataSize());
            sb.append(", heapCost=").append(getHeapCost());
            sb.append(", address=").append(address());
            sb.append(", blockSize=").append(size());
            sb.append('}');
            return sb.toString();
        } else {
            return "NativeMemoryData{ NULL }";
        }
    }
}
