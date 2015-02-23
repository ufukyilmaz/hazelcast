/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.serialization;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.Bits;
import com.hazelcast.util.HashUtil;

import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.UnsafeHelper.BYTE_ARRAY_BASE_OFFSET;

/**
 * @author mdogan 12/10/13
 */
public final class NativeMemoryData extends MemoryBlock implements MutableData {

    // using sign bit as partition hash flag
    static final int PARTITION_HASH_BIT = Integer.SIZE - 1;
    // using highest bit as header flag
    static final int HEADER_BIT = Integer.SIZE - 2;

    static final int TYPE_OFFSET = 0;
    static final int DATA_SIZE_OFFSET = 4;
    static final int DATA_OFFSET = 8;

    static final int HEADER_LENGTH = DATA_OFFSET;

    // max allowed data size is 1GB
    private static final int MAX_DATA_SIZE = 1 << (Integer.SIZE - 2);

    public NativeMemoryData() {
    }

    public NativeMemoryData(long address, int size) {
        super(address, checkDataSize(size));
    }

    private static int checkDataSize(int size) {
        assert Bits.clearBit(Bits.clearBit(size, PARTITION_HASH_BIT), HEADER_BIT) <= MAX_DATA_SIZE;
        return size;
    }

    void setDataSize(int size) {
        checkDataSize(size);
        writeInt(DATA_SIZE_OFFSET, size);
    }

    private void setBytes(byte[] buffer, int offset, int size) {
        setDataSize(size);
        if (size > 0) {
            if (buffer == null) {
                throw new IllegalArgumentException("Buffer is required!");
            }
            if (buffer.length < offset + size) {
                throw new ArrayIndexOutOfBoundsException();
            }
            copyFrom(DATA_OFFSET, buffer, BYTE_ARRAY_BASE_OFFSET + offset, size);
        }
    }

    private void getBytes(byte[] buffer, int offset, int length) {
        copyTo(DATA_OFFSET, buffer, BYTE_ARRAY_BASE_OFFSET + offset, length);
    }

    @Override
    public int dataSize() {
        if (address == 0L) {
            return 0;
        }
        int dataSize = readInt(DATA_SIZE_OFFSET);
        dataSize = Bits.clearBit(dataSize, PARTITION_HASH_BIT);
        dataSize = Bits.clearBit(dataSize, HEADER_BIT);
        return dataSize;
    }

    @Override
    public int getPartitionHash() {
        int dataSize = readInt(DATA_SIZE_OFFSET);
        boolean hasPartitionHash = Bits.isBitSet(dataSize, PARTITION_HASH_BIT);
        if (hasPartitionHash) {
            dataSize = Bits.clearBit(dataSize, PARTITION_HASH_BIT);
            dataSize = Bits.clearBit(dataSize, HEADER_BIT);
            return readInt(DATA_OFFSET + dataSize);
        }
        return hashCode();
    }

    @Override
    public void setPartitionHash(int partitionHash) {
        int current = readInt(DATA_SIZE_OFFSET);
        int dataSize = Bits.clearBit(current, PARTITION_HASH_BIT);
        dataSize = Bits.clearBit(dataSize, HEADER_BIT);

        if (partitionHash != 0) {
            setDataSize(Bits.setBit(current, PARTITION_HASH_BIT));
            writeInt(dataSize + DATA_OFFSET, partitionHash);
        } else {
            setDataSize(Bits.clearBit(current, PARTITION_HASH_BIT));
        }
    }

    @Override
    public boolean hasPartitionHash() {
        int dataSize = readInt(DATA_SIZE_OFFSET);
        return Bits.isBitSet(dataSize, PARTITION_HASH_BIT);
    }

    @Override
    public void setHeader(byte[] header) {
        int current = readInt(DATA_SIZE_OFFSET);
        int dataSize = Bits.clearBit(current, PARTITION_HASH_BIT);
        dataSize = Bits.clearBit(dataSize, HEADER_BIT);

        if (header != null && header.length > 0) {
            int offset = dataSize + DATA_OFFSET;
            if (Bits.isBitSet(current, PARTITION_HASH_BIT)) {
                offset += INT_SIZE_IN_BYTES;
            }
            setDataSize(Bits.setBit(current, HEADER_BIT));
            writeInt(offset, header.length);
            copyFrom(offset + INT_SIZE_IN_BYTES, header, BYTE_ARRAY_BASE_OFFSET, header.length);
        } else {
            setDataSize(Bits.clearBit(current, HEADER_BIT));
        }
    }

    @Override
    public byte[] getData() {
        byte[] buffer = new byte[dataSize()];
        getBytes(buffer, 0, buffer.length);
        return buffer;
    }

    @Override
    public byte[] getHeader() {
        int offset = headerOffset();
        if (offset > 0) {
            int headerSize = readInt(offset);
            byte[] header = new byte[headerSize];
            copyTo(offset + INT_SIZE_IN_BYTES, header, BYTE_ARRAY_BASE_OFFSET, headerSize);
            return header;
        }
        return null;
    }

    @Override
    public int headerSize() {
        int offset = headerOffset();
        return offset > 0 ? readInt(offset) : 0;
    }

    @Override
    public int readIntHeader(int offset, ByteOrder order) {
        long headerOffset = headerOffset() + INT_SIZE_IN_BYTES;
        return readInt(headerOffset + offset, order);
    }

//    @Override
//    public long readLongHeader(int offset, ByteOrder order) {
//        long headerOffset = headerOffset() + LONG_SIZE_IN_BYTES;
//        return readLong(headerOffset + offset, order);
//    }

    private int headerOffset() {
        int current = readInt(DATA_SIZE_OFFSET);
        if (Bits.isBitSet(current, HEADER_BIT)) {
            int dataSize = Bits.clearBit(current, PARTITION_HASH_BIT);
            dataSize = Bits.clearBit(dataSize, HEADER_BIT);

            int offset = dataSize + DATA_OFFSET;
            if (Bits.isBitSet(current, PARTITION_HASH_BIT)) {
                offset += INT_SIZE_IN_BYTES;
            }
            return offset;
        }
        return -1;
    }

    @Override
    public void setData(byte[] array) {
        setBytes(array, 0, array != null ? array.length : 0);
    }

    @Override
    public int getType() {
        if (address == 0L) {
            return SerializationConstants.CONSTANT_TYPE_NULL;
        }
        return readInt(TYPE_OFFSET);
    }

    @Override
    public void setType(int type) {
        writeInt(TYPE_OFFSET, type);
    }

    @Override
    public int getHeapCost() {
        return LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    }

    @Override
    public boolean isPortable() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE == getType();
    }

    //CHECKSTYLE:OFF
    @Override
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

        final int bufferSize = dataSize();
        if (bufferSize != data.dataSize()) {
            return false;
        }

        if (bufferSize == 0) {
            return true;
        }

        if (data instanceof NativeMemoryData) {
            return NativeMemoryDataUtil.equals(address(), ((NativeMemoryData) data).address(), bufferSize);
        }
        byte[] bytes = data.getData();
        return NativeMemoryDataUtil.equals(address(), bufferSize, bytes);
    }
    //CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return HashUtil.MurmurHash3_x86_32_direct(address(), DATA_OFFSET, dataSize());
    }

    @Override
    public long hash64() {
        return HashUtil.MurmurHash3_x64_64_direct(address(), DATA_OFFSET, dataSize());
    }

    public NativeMemoryData reset(long address) {
        setAddress(address);
        if (address > 0L) {
            // tmp size to read data size;
            setSize(INT_SIZE_IN_BYTES + HEADER_LENGTH);
            int size = dataSize() + HEADER_LENGTH;
            if (hasPartitionHash()) {
                size += INT_SIZE_IN_BYTES;
            }
            setSize(size);
        } else {
            setSize(0);
        }
        return this;
    }

    @Override
    public String toString() {
        if (address() > 0L) {
            final StringBuilder sb = new StringBuilder("NativeMemoryData{");
            sb.append("type=").append(getType());
            sb.append(", hashCode=").append(hashCode());
            sb.append(", partitionHash=").append(getPartitionHash());
            sb.append(", dataSize=").append(dataSize());
            sb.append(", heapCost=").append(getHeapCost());
            sb.append(", address=").append(address());
            sb.append(", blockSize=").append(size());
            sb.append('}');
            return sb.toString();
        } else {
            return "OffHeapBinary{ NULL }";
        }
    }
}
