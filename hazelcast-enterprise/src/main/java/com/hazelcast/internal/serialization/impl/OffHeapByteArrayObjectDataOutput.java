package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.OffHeapBits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.MemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.memory.MemoryAccessor.MEM;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

/**
 * Provides methods which let to serialize data directly to the  off-heap
 * <p/>
 * It works like a factory for the memory-blocks creating and returning pointer and sizes
 * It doesn't release memory, memory-releasing is responsibility of the external environment
 */
class OffHeapByteArrayObjectDataOutput implements OffHeapDataOutput {

    protected long pos;
    protected long bufferSize;
    protected long bufferPointer;

    private final long initialSize;
    private final boolean isBigEndian;
    private final EnterpriseSerializationService service;

    OffHeapByteArrayObjectDataOutput(long size, EnterpriseSerializationService service, ByteOrder byteOrder) {
        this.service = service;
        this.initialSize = size;
        this.bufferSize = initialSize;
        this.isBigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
    }

    @Override
    public void write(int b) {
        ensureAvailable(1);
        MEM.putByte(bufferPointer + (pos++), (byte) (b));
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        for (byte b : bytes) {
            write(b);
        }
    }

    @Override
    public void write(long position, int b) {
        MEM.putByte(bufferPointer + position, (byte) (b));
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (len < 0) || ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        ensureAvailable(len);
        MEM.copyMemory(b, ARRAY_BYTE_BASE_OFFSET + off, null, bufferPointer + pos, len);
        pos += len;
    }

    @Override
    public final void writeBoolean(final boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public final void writeBoolean(long position, final boolean v) throws IOException {
        write(position, v ? 1 : 0);
    }

    @Override
    public final void writeByte(final int v) throws IOException {
        write(v);
    }

    @Override
    public final void writeZeroBytes(int count) {
        for (int k = 0; k < count; k++) {
            write(0);
        }
    }

    @Override
    public final void writeByte(long position, final int v) throws IOException {
        write(position, v);
    }

    @Override
    public final void writeBytes(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len);
        for (int i = 0; i < len; i++) {
            MEM.putByte(bufferPointer + (pos++), (byte) s.charAt(i));
        }
    }

    @Override
    public void writeChar(final int v) throws IOException {
        ensureAvailable(CHAR_SIZE_IN_BYTES);
        OffHeapBits.writeChar(bufferPointer, pos, (char) v, isBigEndian);
        pos += CHAR_SIZE_IN_BYTES;
    }

    @Override
    public void writeChar(long position, final int v) throws IOException {
        OffHeapBits.writeChar(bufferPointer, position, (char) v, isBigEndian);
    }

    @Override
    public void writeChars(final String s) throws IOException {
        final int len = s.length();
        ensureAvailable(len * CHAR_SIZE_IN_BYTES);
        for (int i = 0; i < len; i++) {
            final int v = s.charAt(i);
            writeChar(pos, v);
            pos += CHAR_SIZE_IN_BYTES;
        }
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(long position, final double v) throws IOException {
        writeLong(position, Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(double v, ByteOrder byteOrder) throws IOException {
        writeLong(Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeDouble(long position, double v, ByteOrder byteOrder) throws IOException {
        writeLong(position, Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(long position, final float v) throws IOException {
        writeInt(position, Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(float v, ByteOrder byteOrder) throws IOException {
        writeInt(Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeFloat(long position, float v, ByteOrder byteOrder) throws IOException {
        writeInt(position, Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        OffHeapBits.writeInt(bufferPointer, pos, v, isBigEndian);
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(long position, int v) throws IOException {
        OffHeapBits.writeInt(bufferPointer, position, v, isBigEndian);
    }

    @Override
    public void writeInt(int v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(INT_SIZE_IN_BYTES);
        OffHeapBits.writeInt(bufferPointer, pos, v, byteOrder == ByteOrder.BIG_ENDIAN);
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(long position, int v, ByteOrder byteOrder) throws IOException {
        OffHeapBits.writeInt(bufferPointer, position, v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        OffHeapBits.writeLong(bufferPointer, pos, v, isBigEndian);
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(long position, final long v) throws IOException {
        OffHeapBits.writeLong(bufferPointer, position, v, isBigEndian);
    }

    @Override
    public void writeLong(long v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(LONG_SIZE_IN_BYTES);
        OffHeapBits.writeLong(bufferPointer, pos, v, byteOrder == ByteOrder.BIG_ENDIAN);
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(long position, long v, ByteOrder byteOrder) throws IOException {
        OffHeapBits.writeLong(bufferPointer, position, v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeShort(final int v) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        OffHeapBits.writeShort(bufferPointer, pos, (short) v, isBigEndian);
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(long position, final int v) throws IOException {
        OffHeapBits.writeShort(bufferPointer, position, (short) v, isBigEndian);
    }

    @Override
    public void writeShort(int v, ByteOrder byteOrder) throws IOException {
        ensureAvailable(SHORT_SIZE_IN_BYTES);
        OffHeapBits.writeShort(bufferPointer, pos, (short) v, byteOrder == ByteOrder.BIG_ENDIAN);
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(long position, int v, ByteOrder byteOrder) throws IOException {
        OffHeapBits.writeShort(bufferPointer, position, (short) v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeUTF(final String str) throws IOException {
        int len = (str != null) ? str.length() : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            ensureAvailable(len * 3);
            for (int i = 0; i < len; i++) {
                pos += OffHeapBits.writeUtf8Char(bufferPointer, pos, str.charAt(i));
            }
        }
    }

    @Override
    public void writeByteArray(byte[] bytes) throws IOException {
        int len = (bytes != null) ? bytes.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            write(bytes);
        }
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) throws IOException {
        int len = (booleans != null) ? booleans.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (boolean b : booleans) {
                writeBoolean(b);
            }
        }
    }

    @Override
    public void writeCharArray(char[] chars) throws IOException {
        int len = chars != null ? chars.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (char c : chars) {
                writeChar(c);
            }
        }
    }

    @Override
    public void writeIntArray(int[] ints) throws IOException {
        int len = ints != null ? ints.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (int i : ints) {
                writeInt(i);
            }
        }
    }

    @Override
    public void writeLongArray(long[] longs) throws IOException {
        int len = longs != null ? longs.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (long l : longs) {
                writeLong(l);
            }
        }
    }

    @Override
    public void writeDoubleArray(double[] doubles) throws IOException {
        int len = doubles != null ? doubles.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (double d : doubles) {
                writeDouble(d);
            }
        }
    }

    @Override
    public void writeFloatArray(float[] floats) throws IOException {
        int len = floats != null ? floats.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (float f : floats) {
                writeFloat(f);
            }
        }
    }

    @Override
    public void writeShortArray(short[] shorts) throws IOException {
        int len = shorts != null ? shorts.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (short s : shorts) {
                writeShort(s);
            }
        }
    }

    @Override
    public void writeUTFArray(String[] strings) throws IOException {
        int len = strings != null ? strings.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (String s : strings) {
                writeUTF(s);
            }
        }
    }

    final void ensureAvailable(long len) {
        if (available() < len) {
            if (bufferPointer != 0) {
                long newCap = Math.max(bufferSize << 1, bufferSize + len);
                bufferPointer = service.getMemoryManager().reallocate(bufferPointer, bufferSize, newCap);
                bufferSize = newCap;
            } else {
                bufferSize = len > initialSize / 2 ? len * 2 : initialSize;
                bufferPointer = service.getMemoryManager().allocate(bufferSize);
            }
        }
    }

    @Override
    public void writeObject(Object object) throws IOException {
        service.writeObject(this, object);
    }

    @Override
    public void writeData(Data data) throws IOException {
        byte[] payload = data != null ? data.toByteArray() : null;
        writeByteArray(payload);
    }

    /**
     * Returns this buffer's position.
     */
    @Override
    public final long position() {
        return pos;
    }

    @Override
    public void position(long newPos) {
        if ((newPos > bufferSize) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }

        pos = newPos;
    }

    public long available() {
        return bufferPointer != 0 ? bufferSize - pos : 0;
    }

    @Override
    public byte toByteArray()[] {
        throw new IllegalStateException("Not available for offHeap");
    }

    @Override
    public void clear() {
        pos = 0;
        bufferSize = 0;
        bufferPointer = 0;
    }

    @Override
    public void close() {
        pos = 0;

        if (bufferPointer > 0L) {
            service.getMemoryManager().free(bufferPointer, bufferSize);
        }

        bufferSize = 0;
        bufferPointer = 0;
    }

    @Override
    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "OffHeapByteArrayObjectDataOutput{"
                + "size=" + (bufferPointer != 0 ? bufferSize : 0)
                + ", pos=" + pos
                + '}';
    }

    @Override
    public long getPointer() {
        return bufferPointer;
    }

    @Override
    public long getWrittenSize() {
        return pos;
    }

    @Override
    public long getAllocatedSize() {
        return bufferSize;
    }
}
