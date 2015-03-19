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

package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * @author mdogan 12/10/13
 */

public class MemoryBlock {

    protected long address = MemoryManager.NULL_ADDRESS;
    protected int size;

    public MemoryBlock() {
    }

    protected MemoryBlock(long address, int size) {
        this.address = address;
        this.size = size;
    }

    public final byte readByte(long offset) {
        if (offset >= size || offset < 0) { // offset + 1 > size
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset + ", Length: " + 1);
        }
        return UnsafeHelper.UNSAFE.getByte(address + offset);
    }

    public final void writeByte(long offset, byte value) {
        if (offset >= size || offset < 0) { // offset + 1 > size
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset + ", Length: " + 1);
        }
        UnsafeHelper.UNSAFE.putByte(address + offset, value);
    }

    public final int readInt(long offset) {
        if ((offset + INT_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + INT_SIZE_IN_BYTES);
        }
        return UnsafeHelper.UNSAFE.getInt(address + offset);
    }

    public final void writeInt(long offset, int value) {
        if ((offset + INT_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + INT_SIZE_IN_BYTES);
        }
        UnsafeHelper.UNSAFE.putInt(address + offset, value);
    }

    public final long readLong(long offset) {
        if ((offset + LONG_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + LONG_SIZE_IN_BYTES);
        }
        return UnsafeHelper.UNSAFE.getLong(address + offset);
    }

    public final void writeLong(long offset, long value) {
        if ((offset + LONG_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + LONG_SIZE_IN_BYTES);
        }
        UnsafeHelper.UNSAFE.putLong(address + offset, value);
    }

    public final char readChar(long offset) {
        if ((offset + CHAR_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + CHAR_SIZE_IN_BYTES);
        }
        return UnsafeHelper.UNSAFE.getChar(address + offset);
    }

    public final void writeChar(long offset, char value) {
        if ((offset + CHAR_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + CHAR_SIZE_IN_BYTES);
        }
        UnsafeHelper.UNSAFE.putChar(address + offset, value);
    }

    public final double readDouble(long offset) {
        if ((offset + DOUBLE_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + DOUBLE_SIZE_IN_BYTES);
        }
        return UnsafeHelper.UNSAFE.getDouble(address + offset);
    }

    public final void writeDouble(long offset, double value) {
        if ((offset + DOUBLE_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + DOUBLE_SIZE_IN_BYTES);
        }
        UnsafeHelper.UNSAFE.putDouble(address + offset, value);
    }

    public final float readFloat(long offset) {
        if ((offset + FLOAT_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + FLOAT_SIZE_IN_BYTES);
        }
        return UnsafeHelper.UNSAFE.getFloat(address + offset);
    }

    public final void writeFloat(long offset, float value) {
        if ((offset + FLOAT_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + FLOAT_SIZE_IN_BYTES);
        }
        UnsafeHelper.UNSAFE.putFloat(address + offset, value);
    }

    public final short readShort(long offset) {
        if ((offset + SHORT_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: " + offset
                    + ", Length: " + SHORT_SIZE_IN_BYTES);
        }
        return UnsafeHelper.UNSAFE.getShort(address + offset);
    }

    public final void writeShort(long offset, short value) {
        if ((offset + SHORT_SIZE_IN_BYTES) > size || offset < 0) {
            throw new IndexOutOfBoundsException("Size: " + size + ", Offset: "
                    + offset + ", Length: " + SHORT_SIZE_IN_BYTES);
        }
        UnsafeHelper.UNSAFE.putShort(address + offset, value);
    }

    public final void copyFrom(long destinationOffset, Object source, long offset, int length) {
        if ((destinationOffset + length) > size || destinationOffset < 0 || offset < 0) {
            throw new IndexOutOfBoundsException("Destination offset: " + destinationOffset
                    + ", length: " + length + ", size: " + size);
        }

        long realAddress = address + destinationOffset;
        while (length > 0) {
            int chunk = (length > UnsafeHelper.MEM_COPY_THRESHOLD) ? UnsafeHelper.MEM_COPY_THRESHOLD : length;
            UnsafeHelper.UNSAFE.copyMemory(source, offset, null, realAddress, chunk);
            length -= chunk;
            offset += chunk;
            realAddress += chunk;
        }
    }

    public final void copyTo(long sourceOffset, Object destination, long offset, int length) {
        if ((sourceOffset + length) > size || sourceOffset < 0  || offset < 0) {
            throw new IndexOutOfBoundsException("Source offset: " + sourceOffset
                    + ", length: " + length + ", size: " + size);
        }

        long realAddress = address + sourceOffset;
        while (length > 0) {
            int chunk = (length > UnsafeHelper.MEM_COPY_THRESHOLD) ? UnsafeHelper.MEM_COPY_THRESHOLD : length;
            UnsafeHelper.UNSAFE.copyMemory(null, realAddress, destination, offset, chunk);
            length -= chunk;
            offset += chunk;
            realAddress += chunk;
        }
    }

    public final void zero() {
        UnsafeHelper.UNSAFE.setMemory(address, size, (byte) 0);
    }

    public final long address() {
        return address;
    }

    public final int size() {
        return size;
    }

    protected final void setAddress(long address) {
        this.address = address;
    }

    protected final void setSize(int size) {
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemoryBlock block = (MemoryBlock) o;

        if (address != block.address) {
            return false;
        }
        if (size != block.size) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (address ^ (address >>> 32));
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MemoryBlock{");
        sb.append("address=").append(address);
        sb.append(", size=").append(size);
        sb.append('}');
        return sb.toString();
    }
}
