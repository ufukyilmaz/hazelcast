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
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.UTFEncoderDecoder;
import com.hazelcast.nio.UnsafeHelper;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
* @author mdogan 06/16/13
*/
final class MemoryBlockDataInput extends InputStream implements EnterpriseBufferObjectDataInput {

    private MemoryBlock memory;

    private final int size;

    private int pos;

    private int mark;

    private final EnterpriseSerializationService service;

    private byte[] utfBuffer;

    MemoryBlockDataInput(MemoryBlock memoryBlock, int offset, EnterpriseSerializationService serializationService) {
        memory = memoryBlock;
        size = memoryBlock.size();
        service = serializationService;
        pos = offset;
    }

    public int read() throws IOException {
        return (pos < size) ? memory.readByte(pos++) : -1;
    }

    public int read(int position) throws IOException {
        return (position < size) ? memory.readByte(position) : -1;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (len <= 0) {
            return 0;
        }
        if (pos >= size) {
            return -1;
        }
        if (pos + len > size) {
            len = size - pos;
        }
        memCopy(b, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + off, len, UnsafeHelper.BYTE_ARRAY_INDEX_SCALE);
        return len;
    }

    public boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    public boolean readBoolean(int position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    /**
     * See the general contract of the <code>readByte</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit
     *         <code>byte</code>.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    public byte readByte(int position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    public char readChar() throws IOException {
        char c = readChar(pos);
        pos += CHAR_SIZE_IN_BYTES;
        return c;
    }

    public char readChar(int position) throws IOException {
        try {
            return memory.readChar(position);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    public double readDouble() throws IOException {
        final double d = readDouble(pos);
        pos += DOUBLE_SIZE_IN_BYTES;
        return d;
    }

    public double readDouble(int position) throws IOException {
        try {
            return memory.readDouble(position);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public EnterpriseSerializationService getSerializationService() {
        return service;
    }

    public double readDouble(ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            return Double.longBitsToDouble(readLong(byteOrder));
        }
        return readDouble();
    }

    @Override
    public double readDouble(int position, ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            return Double.longBitsToDouble(readLong(position, byteOrder));
        }
        return readDouble(position);
    }

    public float readFloat() throws IOException {
        final float f = readFloat(pos);
        pos += FLOAT_SIZE_IN_BYTES;
        return f;
    }

    public float readFloat(int position) throws IOException {
        try {
            return memory.readFloat(position);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public float readFloat(ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            return Float.intBitsToFloat(readInt(byteOrder));
        }
        return readFloat();
    }

    @Override
    public float readFloat(int position, ByteOrder byteOrder) throws IOException {
        if (byteOrder != ByteOrder.nativeOrder()) {
            return Float.intBitsToFloat(readInt(position, byteOrder));
        }
        return readFloat(position);
    }

    public int readInt() throws IOException {
        int i = readInt(pos);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    public int readInt(int position) throws IOException {
        try {
            return memory.readInt(position);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException {
        int v = readInt();
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Integer.reverseBytes(v);
        }
        return v;
    }

    @Override
    public int readInt(int position, ByteOrder byteOrder) throws IOException {
        int v = readInt(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Integer.reverseBytes(v);
        }
        return v;
    }

    public long readLong() throws IOException {
        final long l = readLong(pos);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    public long readLong(int position) throws IOException {
        try {
            return memory.readLong(position);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException {
        long v = readLong();
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Long.reverseBytes(v);
        }
        return v;
    }

    @Override
    public long readLong(int position, ByteOrder byteOrder) throws IOException {
        long v = readLong(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Long.reverseBytes(v);
        }
        return v;
    }

    public short readShort() throws IOException {
        short s = readShort(pos);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    public short readShort(int position) throws IOException {
        try {
            return memory.readShort(position);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public short readShort(ByteOrder byteOrder) throws IOException {
        short v = readShort();
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Short.reverseBytes(v);
        }
        return v;
    }

    @Override
    public short readShort(int position, ByteOrder byteOrder) throws IOException {
        short v = readShort(position);
        if (byteOrder != ByteOrder.nativeOrder()) {
            v = Short.reverseBytes(v);
        }
        return v;
    }

    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            char[] values = new char[len];
            memCopy(values, UnsafeHelper.CHAR_ARRAY_BASE_OFFSET, len, UnsafeHelper.CHAR_ARRAY_INDEX_SCALE);
            return values;
        }
        return new char[0];
    }

    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            int[] values = new int[len];
            memCopy(values, UnsafeHelper.INT_ARRAY_BASE_OFFSET, len, UnsafeHelper.INT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new int[0];
    }

    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            long[] values = new long[len];
            memCopy(values, UnsafeHelper.LONG_ARRAY_BASE_OFFSET, len, UnsafeHelper.LONG_ARRAY_INDEX_SCALE);
            return values;
        }
        return new long[0];
    }

    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            double[] values = new double[len];
            memCopy(values, UnsafeHelper.DOUBLE_ARRAY_BASE_OFFSET, len, UnsafeHelper.DOUBLE_ARRAY_INDEX_SCALE);
            return values;
        }
        return new double[0];
    }

    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            float[] values = new float[len];
            memCopy(values, UnsafeHelper.FLOAT_ARRAY_BASE_OFFSET, len, UnsafeHelper.FLOAT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new float[0];
    }

    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len > 0) {
            short[] values = new short[len];
            memCopy(values, UnsafeHelper.SHORT_ARRAY_BASE_OFFSET, len, UnsafeHelper.SHORT_ARRAY_INDEX_SCALE);
            return values;
        }
        return new short[0];
    }

    private void memCopy(final Object dest, final long destOffset, final int length, final int indexScale) throws IOException {
        if (length < 0) {
            throw new NegativeArraySizeException("Destination length is negative: " + length);
        }

        int actualLength = length * indexScale;
        try {
            memory.copyTo(pos, dest, destOffset, actualLength);
            pos += actualLength;
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }

    public void readFully(final byte[] b) throws IOException {
        int r = read(b);
        if (r != b.length) {
            throw new EOFException();
        }
    }

    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        int r = read(b, off, len);
        if (r != len) {
            throw new EOFException();
        }
    }

    /**
     * See the general contract of the <code>readUnsignedByte</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned
     * 8-bit number.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    /**
     * See the general contract of the <code>readUnsignedShort</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an
     * unsigned 16-bit integer.
     * @throws java.io.EOFException if this input stream reaches the end before reading two
     *                              bytes.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    @Deprecated
    public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * See the general contract of the <code>readUTF</code> method of
     * <code>DataInput</code>.
     * <p/>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return a Unicode string.
     * @throws java.io.EOFException           if this input stream reaches the end before reading all
     *                                the bytes.
     * @throws java.io.IOException            if an I/O error occurs.
     * @throws java.io.UTFDataFormatException if the bytes do not represent a valid modified UTF-8
     *                                encoding of a string.
     * @see java.io.DataInputStream#readUTF(java.io.DataInput)
     */
    public String readUTF() throws IOException {
        if (utfBuffer == null) {
            utfBuffer = new byte[UTF_BUFFER_SIZE];
        }
        return UTFEncoderDecoder.readUTF(this, utfBuffer);
    }

    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Object readObject() throws IOException {
        return service.readObject(this);
    }

    @Override
    public Data readData() throws IOException {
        return service.readData(this);
    }

    @Override
    public Data readData(DataType type) throws IOException {
        return service.readData(this, type);
    }

    @Override
    public Data tryReadData(DataType type) throws IOException {
        return service.tryReadData(this, type);
    }

    @Override
    public long skip(long n) {
        if (n <= 0 || n >= Integer.MAX_VALUE) {
            return 0L;
        }
        return skipBytes((int) n);
    }

    public int skipBytes(final int n) {
        if (n <= 0) {
            return 0;
        }
        int skip = n;
        final int pos = position();
        if (pos + skip > size) {
            skip = size - pos;
        }
        position(pos + skip);
        return skip;
    }

    /**
     * Returns this buffer's position.
     */
    public int position() {
        return pos;
    }

    public void position(int newPos) {
        if ((newPos > size) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }
        pos = newPos;
        if (mark > pos) {
            mark = -1;
        }
    }

    @Override
    public int available() {
        return size - pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        mark = pos;
    }

    @Override
    public void reset() {
        pos = mark;
    }

    @Override
    public void close() {
        memory = null;
    }

    @Override
    public ClassLoader getClassLoader() {
        return service.getClassLoader();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MemoryBlockDataInput");
        sb.append("{size=").append(size);
        sb.append(", pos=").append(pos);
        sb.append(", mark=").append(mark);
        sb.append(", byteOrder=").append(getByteOrder());
        sb.append('}');
        return sb.toString();
    }
}
