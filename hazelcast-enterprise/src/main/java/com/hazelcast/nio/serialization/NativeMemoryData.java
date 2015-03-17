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
import com.hazelcast.util.HashUtil;

import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.UnsafeHelper.BYTE_ARRAY_BASE_OFFSET;

/**
 * @author mdogan 12/10/13
 */
public final class NativeMemoryData extends MemoryBlock implements Data {

    static final int SIZE_OFFSET = 0;
    static final int TYPE_OFFSET = 4;
    // we can store this bit in sign-bit of data-size
    // for the sake of simplicity and make structure same as DefaultData
    // we will use a byte to store partition_hash bit
    static final int PARTITION_HASH_BIT_OFFSET = 8;
    static final int DATA_OFFSET = 9;

    static final int NATIVE_HEADER_OVERHEAD = TYPE_OFFSET - DefaultData.TYPE_OFFSET;

    private static final boolean BIG_ENDIAN = ByteOrder.BIG_ENDIAN == ByteOrder.nativeOrder();

    public NativeMemoryData() {
    }

    public NativeMemoryData(long address, int size) {
        super(address, size);
    }

    @Override
    public int totalSize() {
        if (address == 0L) {
            return 0;
        }
        return readInt(SIZE_OFFSET);
    }

    @Override
    public int dataSize() {
        return Math.max(totalSize() - DATA_OFFSET + NATIVE_HEADER_OVERHEAD, 0);
    }

    @Override
    public int getPartitionHash() {
        if (hasPartitionHash()) {
            int size = totalSize();
            int hash = readInt(size);
            return BIG_ENDIAN ? hash : Integer.reverseBytes(hash);
        }
        return hashCode();
    }

    @Override
    public boolean hasPartitionHash() {
        if (address == 0L) {
            return false;
        }
        return readByte(PARTITION_HASH_BIT_OFFSET) != 0;
    }

    @Override
    public byte[] toByteArray() {
        int len = totalSize();
        byte[] buffer = new byte[len];
        copyTo(TYPE_OFFSET, buffer, BYTE_ARRAY_BASE_OFFSET, len);
        return buffer;
    }

    @Override
    public int getType() {
        if (address == 0L) {
            return SerializationConstants.CONSTANT_TYPE_NULL;
        }
        int type = readInt(TYPE_OFFSET);
        return BIG_ENDIAN ? type : Integer.reverseBytes(type);
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
    //CHECKSTYLE:ON

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
            setSize(INT_SIZE_IN_BYTES + DATA_OFFSET);
            int size = dataSize() + DATA_OFFSET;
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
