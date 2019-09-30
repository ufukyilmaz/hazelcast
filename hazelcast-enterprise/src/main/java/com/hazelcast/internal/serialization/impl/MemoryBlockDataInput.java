package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BOOLEAN_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BOOLEAN_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_CHAR_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_CHAR_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_DOUBLE_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_DOUBLE_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_FLOAT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_FLOAT_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_INT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_INT_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_LONG_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_LONG_INDEX_SCALE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_SHORT_BASE_OFFSET;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_SHORT_INDEX_SCALE;
import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.UTF_8;

@SuppressWarnings("checkstyle:methodcount")
final class MemoryBlockDataInput extends VersionedObjectDataInput implements EnterpriseBufferObjectDataInput {

    private static final int LOWER_BYTE_MASK = (1 << Byte.SIZE) - 1;
    private static final int LOWER_SHORT_MASK = (1 << Short.SIZE) - 1;

    private MemoryBlock memory;

    private final int size;

    private final int offset;

    private int pos;

    private int mark;

    private final EnterpriseSerializationService service;

    MemoryBlockDataInput(MemoryBlock memoryBlock, int position, int offset, EnterpriseSerializationService serializationService) {
        memory = memoryBlock;
        size = memoryBlock.size();
        service = serializationService;
        pos = position;
        this.offset = offset;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init(byte[] data, int offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read() throws IOException {
        return (pos + offset < size) ? memory.readByte((pos++) + offset) : -1;
    }

    @Override
    public int read(int position) throws IOException {
        return (position + offset < size) ? memory.readByte(position + offset) : -1;
    }

    @Override
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
        if (pos + offset >= size) {
            return -1;
        }
        if (pos + offset + len > size) {
            len = size - pos;
        }
        memCopy(b, ARRAY_BYTE_BASE_OFFSET + off, len, ARRAY_BYTE_INDEX_SCALE);
        return len;
    }

    @Override
    public boolean readBoolean() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    @Override
    public boolean readBoolean(int position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    /**
     * See the general contract of the {@code readByte} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit {@code byte}.
     * @throws java.io.EOFException if this input stream has reached the end.
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public byte readByte() throws IOException {
        final int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    @Override
    public byte readByte(int position) throws IOException {
        final int ch = read(position);
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    @Override
    public char readChar() throws IOException {
        char c = readChar(pos);
        pos += CHAR_SIZE_IN_BYTES;
        return c;
    }

    @Override
    public char readChar(int position) throws IOException {
        try {
            return memory.readChar(position + offset);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public double readDouble() throws IOException {
        final double d = readDouble(pos);
        pos += DOUBLE_SIZE_IN_BYTES;
        return d;
    }

    @Override
    public double readDouble(int position) throws IOException {
        try {
            return memory.readDouble(position + offset);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public EnterpriseSerializationService getSerializationService() {
        return service;
    }

    @Override
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

    @Override
    public float readFloat() throws IOException {
        final float f = readFloat(pos);
        pos += FLOAT_SIZE_IN_BYTES;
        return f;
    }

    @Override
    public float readFloat(int position) throws IOException {
        try {
            return memory.readFloat(position + offset);
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

    @Override
    public int readInt() throws IOException {
        int i = readInt(pos);
        pos += INT_SIZE_IN_BYTES;
        return i;
    }

    @Override
    public int readInt(int position) throws IOException {
        try {
            return memory.readInt(position + offset);
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

    @Override
    public long readLong() throws IOException {
        final long l = readLong(pos);
        pos += LONG_SIZE_IN_BYTES;
        return l;
    }

    @Override
    public long readLong(int position) throws IOException {
        try {
            return memory.readLong(position + offset);
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

    @Override
    public short readShort() throws IOException {
        short s = readShort(pos);
        pos += SHORT_SIZE_IN_BYTES;
        return s;
    }

    @Override
    public short readShort(int position) throws IOException {
        try {
            return memory.readShort(position + offset);
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

    @Override
    public byte[] readByteArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];
    }

    @Override
    public boolean[] readBooleanArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            boolean[] values = new boolean[len];
            memCopy(values, ARRAY_BOOLEAN_BASE_OFFSET, len, ARRAY_BOOLEAN_INDEX_SCALE);
            return values;
        }
        return new boolean[0];
    }

    @Override
    public char[] readCharArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            char[] values = new char[len];
            memCopy(values, ARRAY_CHAR_BASE_OFFSET, len, ARRAY_CHAR_INDEX_SCALE);
            return values;
        }
        return new char[0];
    }

    @Override
    public int[] readIntArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            int[] values = new int[len];
            memCopy(values, ARRAY_INT_BASE_OFFSET, len, ARRAY_INT_INDEX_SCALE);
            return values;
        }
        return new int[0];
    }

    @Override
    public long[] readLongArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            long[] values = new long[len];
            memCopy(values, ARRAY_LONG_BASE_OFFSET, len, ARRAY_LONG_INDEX_SCALE);
            return values;
        }
        return new long[0];
    }

    @Override
    public double[] readDoubleArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            double[] values = new double[len];
            memCopy(values, ARRAY_DOUBLE_BASE_OFFSET, len, ARRAY_DOUBLE_INDEX_SCALE);
            return values;
        }
        return new double[0];
    }

    @Override
    public float[] readFloatArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            float[] values = new float[len];
            memCopy(values, ARRAY_FLOAT_BASE_OFFSET, len, ARRAY_FLOAT_INDEX_SCALE);
            return values;
        }
        return new float[0];
    }

    @Override
    public short[] readShortArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            short[] values = new short[len];
            memCopy(values, ARRAY_SHORT_BASE_OFFSET, len, ARRAY_SHORT_INDEX_SCALE);
            return values;
        }
        return new short[0];
    }

    @Override
    public String[] readUTFArray() throws IOException {
        int len = readInt();
        if (len == NULL_ARRAY_LENGTH) {
            return null;
        }
        if (len > 0) {
            String[] values = new String[len];
            for (int i = 0; i < len; i++) {
                values[i] = readUTF();
            }
            return values;
        }
        return new String[0];
    }

    @Override
    public <T> T readDataAsObject() throws IOException {
        // a future optimization would be to skip the construction of the Data object.
        Data data = readData();
        return data == null ? null : (T) service.toObject(data);
    }

    private void memCopy(final Object dest, final long destOffset, final int length, final int indexScale)
            throws IOException {
        if (length < 0) {
            throw new NegativeArraySizeException("Destination length is negative: " + length);
        }

        int actualLength = length * indexScale;
        try {
            memory.copyTo(pos + offset, dest, destOffset, actualLength);
            pos += actualLength;
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        int r = read(b);
        if (r != b.length) {
            throw new EOFException();
        }
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        int r = read(b, off, len);
        if (r != len) {
            throw new EOFException();
        }
    }

    /**
     * See the general contract of the {@code readUnsignedByte} method of
     * {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned 8-bit number
     * @throws java.io.EOFException if this input stream has reached the end
     * @throws java.io.IOException  if an I/O error occurs
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & LOWER_BYTE_MASK;
    }

    /**
     * See the general contract of the {@code readUnsignedShort} method of
     * {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an unsigned 16-bit integer
     * @throws java.io.EOFException if this input stream reaches the end before reading two bytes
     * @throws java.io.IOException  if an I/O error occurs.
     * @see java.io.FilterInputStream#in
     */
    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & LOWER_SHORT_MASK;
    }

    @Override
    @Deprecated
    public String readLine() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * See the general contract of the {@code readUTF} method of {@code DataInput}.
     * <p>
     * Bytes for this operation are read from the contained input stream.
     *
     * @return a Unicode string
     * @throws java.io.EOFException           if this input stream reaches the end before reading all the bytes
     * @throws java.io.IOException            if an I/O error occurs
     * @throws java.io.UTFDataFormatException if the bytes do not represent a valid modified UTF-8 encoding of a string
     * @see java.io.DataInputStream#readUTF(java.io.DataInput)
     */
    @Override
    public String readUTF() throws IOException {
        int numberOfBytes = readInt();
        if (numberOfBytes == NULL_ARRAY_LENGTH) {
            return null;
        }

        byte[] utf8Bytes = new byte[numberOfBytes];
        memory.copyToByteArray(pos + offset, utf8Bytes, 0, numberOfBytes);
        position(pos + numberOfBytes);
        return new String(utf8Bytes, UTF_8);
    }

    @Override
    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object readObject() throws IOException {
        return service.readObject(this);
    }

    @Override
    public <T> T readObject(Class aClass)
            throws IOException {
        return service.readObject(this, aClass);
    }

    @Override
    public Data readData() throws IOException {
        byte[] bytes = readByteArray();
        Data data = bytes != null ? new HeapData(bytes) : null;
        return data;
    }

    @Override
    public Data readData(DataType type) throws IOException {
        return EnterpriseSerializationUtil.readDataInternal(this, type, service.getCurrentMemoryAllocator(), false);
    }

    @Override
    public Data tryReadData(DataType type) throws IOException {
        return EnterpriseSerializationUtil.readDataInternal(this, type, service.getCurrentMemoryAllocator(), true);
    }

    @Override
    public long skip(long n) {
        if (n <= 0 || n >= Integer.MAX_VALUE) {
            return 0L;
        }
        return skipBytes((int) n);
    }

    @Override
    public int skipBytes(final int n) {
        if (n <= 0) {
            return 0;
        }
        int skip = n;
        final int pos = position();
        if (pos + skip > size - offset) {
            skip = size - offset - pos;
        }
        position(pos + skip);
        return skip;
    }

    /**
     * Returns this buffer's position.
     */
    @Override
    public int position() {
        return pos;
    }

    @Override
    public void position(int newPos) {
        if ((newPos > size - offset) || (newPos < 0)) {
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
        return "MemoryBlockDataInput{size=" + size + ", pos=" + pos
                + ", mark=" + mark + ", byteOrder=" + getByteOrder() + '}';
    }
}
