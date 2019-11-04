package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.ArrayUtils;
import com.hazelcast.nio.serialization.Data;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteOrder;

import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;
import static com.hazelcast.internal.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.UTF_8;
import static com.hazelcast.version.Version.UNKNOWN;

/**
 * FixedBufferObjectDataOutput is specialized {@code BufferObjectDataOutput}
 * implementation with fixed buffer capacity. When capacity is reached,
 * further writes are ignored and not added to the buffer but still position advances.
 * <p>
 * If {@linkplain #position()} is greater than {@linkplain #capacity()},
 * it means buffer capacity is exceeded.
 *
 * FixedBufferObjectDataOutput can be used as a pre-allocated in-memory BufferObjectDataOutput
 * for the hot path serializations. If serialized form of the object exceeds the capacity
 * then a fallback serialization method can be invoked.
 * <pre>
 * FixedBufferObjectDataOutput fixedDataOut = ...;
 *
 * // Original output stream
 * ObjectDataOutput dataOut = ...;
 *
 * fixedDataOut.writeObject(obj);
 * int length = fixedDataOut.position();
 *
 * if (length > entryDataOut.capacity()) {
 *     // Buffer overflow
 *     // Serialize directly to the original stream
 *     dataOut.writeObject(entry);
 * } else {
 *     dataOut.write(entryDataOut.getBuffer(), 0, length);
 * }
 * </pre>
 */
@SuppressWarnings("checkstyle:methodcount")
public class FixedBufferObjectDataOutput extends VersionedObjectDataOutput implements BufferObjectDataOutput {

    private final InternalSerializationService service;
    private final boolean isBigEndian;
    private final byte[] buffer;
    private int pos;

    public FixedBufferObjectDataOutput(int size, InternalSerializationService service, ByteOrder byteOrder) {
        this.buffer = new byte[size];
        this.service = service;
        isBigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
    }

    @Override
    public void write(int b) {
        if (isAvailable(1)) {
            buffer[pos] = (byte) (b);
        }
        pos++;
    }

    @Override
    public void write(int position, int b) {
        buffer[position] = (byte) b;
    }

    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ArrayUtils.boundsCheck(b.length, off, len);
        if (len == 0) {
            return;
        }
        if (isAvailable(len)) {
            System.arraycopy(b, off, buffer, pos, len);
        }
        pos += len;
    }

    @Override
    public final void writeBoolean(final boolean v) {
        write(v ? 1 : 0);
    }

    @Override
    public final void writeBoolean(int position, final boolean v) {
        write(position, v ? 1 : 0);
    }

    @Override
    public final void writeByte(final int v) {
        write(v);
    }

    @Override
    public final void writeZeroBytes(int count) {
        for (int k = 0; k < count; k++) {
            write(0);
        }
    }

    @Override
    public final void writeByte(int position, final int v) {
        write(position, v);
    }

    @Override
    public final void writeBytes(final String s) {
        int len = s.length();
        if (isAvailable(len)) {
            for (int i = 0; i < len; i++) {
                buffer[pos + i] = (byte) s.charAt(i);
            }
        }
        pos += len;
    }

    @Override
    public void writeChar(final int v) {
        if (isAvailable(CHAR_SIZE_IN_BYTES)) {
            Bits.writeChar(buffer, pos, (char) v, isBigEndian);
        }
        pos += CHAR_SIZE_IN_BYTES;
    }

    @Override
    public void writeChar(int position, final int v) {
        Bits.writeChar(buffer, position, (char) v, isBigEndian);
    }

    @Override
    public void writeChars(final String s) {
        final int len = s.length();
        if (isAvailable(len * CHAR_SIZE_IN_BYTES)) {
            for (int i = 0; i < len; i++) {
                final int v = s.charAt(i);
                writeChar(pos, v);
            }
        }
        pos += (len * CHAR_SIZE_IN_BYTES);
    }

    @Override
    public void writeDouble(final double v) {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(int position, final double v) {
        writeLong(position, Double.doubleToLongBits(v));
    }

    @Override
    public void writeDouble(double v, ByteOrder byteOrder) {
        writeLong(Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeDouble(int position, double v, ByteOrder byteOrder) {
        writeLong(position, Double.doubleToLongBits(v), byteOrder);
    }

    @Override
    public void writeFloat(final float v) {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(int position, final float v) {
        writeInt(position, Float.floatToIntBits(v));
    }

    @Override
    public void writeFloat(float v, ByteOrder byteOrder) {
        writeInt(Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeFloat(int position, float v, ByteOrder byteOrder) {
        writeInt(position, Float.floatToIntBits(v), byteOrder);
    }

    @Override
    public void writeInt(final int v) {
        if (isAvailable(INT_SIZE_IN_BYTES)) {
            Bits.writeInt(buffer, pos, v, isBigEndian);
        }
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(int position, int v) {
        Bits.writeInt(buffer, position, v, isBigEndian);
    }

    @Override
    public void writeInt(int v, ByteOrder byteOrder) {
        if (isAvailable(INT_SIZE_IN_BYTES)) {
            Bits.writeInt(buffer, pos, v, byteOrder == ByteOrder.BIG_ENDIAN);
        }
        pos += INT_SIZE_IN_BYTES;
    }

    @Override
    public void writeInt(int position, int v, ByteOrder byteOrder) {
        Bits.writeInt(buffer, position, v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeLong(final long v) {
        if (isAvailable(LONG_SIZE_IN_BYTES)) {
            Bits.writeLong(buffer, pos, v, isBigEndian);
        }
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(int position, final long v) {
        Bits.writeLong(buffer, position, v, isBigEndian);
    }

    @Override
    public void writeLong(long v, ByteOrder byteOrder) {
        if (isAvailable(LONG_SIZE_IN_BYTES)) {
            Bits.writeLong(buffer, pos, v, byteOrder == ByteOrder.BIG_ENDIAN);
        }
        pos += LONG_SIZE_IN_BYTES;
    }

    @Override
    public void writeLong(int position, long v, ByteOrder byteOrder) {
        Bits.writeLong(buffer, position, v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeShort(final int v) {
        if (isAvailable(SHORT_SIZE_IN_BYTES)) {
            Bits.writeShort(buffer, pos, (short) v, isBigEndian);
        }
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(int position, final int v) {
        Bits.writeShort(buffer, position, (short) v, isBigEndian);
    }

    @Override
    public void writeShort(int v, ByteOrder byteOrder) {
        if (isAvailable(SHORT_SIZE_IN_BYTES)) {
            Bits.writeShort(buffer, pos, (short) v, byteOrder == ByteOrder.BIG_ENDIAN);
        }
        pos += SHORT_SIZE_IN_BYTES;
    }

    @Override
    public void writeShort(int position, int v, ByteOrder byteOrder) {
        Bits.writeShort(buffer, position, (short) v, byteOrder == ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void writeUTF(final String str) {
        if (str == null) {
            writeInt(NULL_ARRAY_LENGTH);
            return;
        }

        byte[] utf8Bytes = str.getBytes(UTF_8);
        writeInt(utf8Bytes.length);
        write(utf8Bytes);
    }

    @Override
    public void writeByteArray(byte[] bytes) {
        int len = (bytes != null) ? bytes.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            write(bytes);
        }
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) {
        int len = (booleans != null) ? booleans.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (boolean b : booleans) {
                writeBoolean(b);
            }
        }
    }

    @Override
    public void writeCharArray(char[] chars) {
        int len = chars != null ? chars.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (char c : chars) {
                writeChar(c);
            }
        }
    }

    @Override
    public void writeIntArray(int[] ints) {
        int len = ints != null ? ints.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (int i : ints) {
                writeInt(i);
            }
        }
    }

    @Override
    public void writeLongArray(long[] longs) {
        int len = longs != null ? longs.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (long l : longs) {
                writeLong(l);
            }
        }
    }

    @Override
    public void writeDoubleArray(double[] doubles) {
        int len = doubles != null ? doubles.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (double d : doubles) {
                writeDouble(d);
            }
        }
    }

    @Override
    public void writeFloatArray(float[] floats) {
        int len = floats != null ? floats.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (float f : floats) {
                writeFloat(f);
            }
        }
    }

    @Override
    public void writeShortArray(short[] shorts) {
        int len = shorts != null ? shorts.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (short s : shorts) {
                writeShort(s);
            }
        }
    }

    @Override
    public void writeUTFArray(String[] strings) {
        int len = strings != null ? strings.length : NULL_ARRAY_LENGTH;
        writeInt(len);
        if (len > 0) {
            for (String s : strings) {
                writeUTF(s);
            }
        }
    }

    private boolean isAvailable(int len) {
        return buffer.length - pos >= len;
    }

    @Override
    public void writeObject(Object object) {
        service.writeObject(this, object);
    }

    @Override
    public void writeData(Data data) {
        int len = data == null ? NULL_ARRAY_LENGTH : data.totalSize();
        writeInt(len);
        if (len > 0) {
            if (isAvailable(len)) {
                data.copyTo(buffer, pos);
            }
            pos += len;
        }
    }

    @Override
    public final int position() {
        return pos;
    }

    @Override
    public void position(int newPos) {
        if ((newPos > pos) || (newPos < 0)) {
            throw new IllegalArgumentException();
        }

        pos = newPos;
    }

    @Override
    public byte[] toByteArray() {
        return toByteArray(0);
    }

    @Override
    public byte[] toByteArray(int padding) {
        if (pos == 0) {
            return new byte[padding];
        }

        final byte[] newBuffer = new byte[padding + pos];
        System.arraycopy(buffer, 0, newBuffer, padding, pos);
        return newBuffer;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getBuffer() {
        return buffer;
    }

    public int capacity() {
        return buffer.length;
    }

    @Override
    public void clear() {
        pos = 0;
        version = UNKNOWN;
        wanProtocolVersion = UNKNOWN;
    }

    @Override
    public void close() {
        clear();
    }

    @Override
    public ByteOrder getByteOrder() {
        return isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public SerializationService getSerializationService() {
        return service;
    }

    @Override
    public String toString() {
        return "FixedBufferDataOutput{" + "capacity=" + buffer.length + ", isBigEndian=" + isBigEndian + ", pos=" + pos + '}';
    }
}
