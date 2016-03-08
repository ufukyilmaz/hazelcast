package com.hazelcast.nio;

import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.internal.serialization.impl.OffHeapDataInput;

import java.io.IOException;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.NATIVE_ACCESS;

public final class OffHeapBits {

    private OffHeapBits() {
    }

    public static char readChar(long bufferPointer, long pos, boolean bigEndian) {
        return bigEndian ? readCharB(bufferPointer, pos) : readCharL(bufferPointer, pos);
    }

    public static char readCharB(long bufferPointer, long pos) {
        return EndiannessUtil.readCharB(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static char readCharL(long bufferPointer, long pos) {
        return EndiannessUtil.readCharL(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static void writeChar(long bufferPointer, long pos, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharB(bufferPointer, pos, v);
        } else {
            writeCharL(bufferPointer, pos, v);
        }
    }

    public static void writeCharB(long bufferPointer, long pos, char v) {
        EndiannessUtil.writeCharB(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }

    public static void writeCharL(long bufferPointer, long pos, char v) {
        EndiannessUtil.writeCharL(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }




    public static short readShort(long bufferPointer, long pos, boolean bigEndian) {
        return bigEndian ? readShortB(bufferPointer, pos) : readShortL(bufferPointer, pos);
    }

    public static short readShortB(long bufferPointer, long pos) {
        return EndiannessUtil.readShortB(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static short readShortL(long bufferPointer, long pos) {
        return EndiannessUtil.readShortL(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static void writeShort(long bufferPointer, long pos, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(bufferPointer, pos, v);
        } else {
            writeShortL(bufferPointer, pos, v);
        }
    }

    public static void writeShortB(long bufferPointer, long pos, short v) {
        EndiannessUtil.writeShortB(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }

    public static void writeShortL(long bufferPointer, long pos, short v) {
        EndiannessUtil.writeShortL(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }




    public static int readInt(long bufferPointer, long pos, boolean bigEndian) {
        return bigEndian ? readIntB(bufferPointer, pos) : readIntL(bufferPointer, pos);
    }

    public static int readIntB(long bufferPointer, long pos) {
        return EndiannessUtil.readIntB(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static int readIntL(long bufferPointer, long pos) {
        return EndiannessUtil.readIntL(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static void writeInt(long bufferPointer, long pos, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntB(bufferPointer, pos, v);
        } else {
            writeIntL(bufferPointer, pos, v);
        }
    }

    public static void writeIntB(long bufferPointer, long pos, int v) {
        EndiannessUtil.writeIntB(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }

    public static void writeIntL(long bufferPointer, long pos, int v) {
        EndiannessUtil.writeIntL(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }




    public static long readLong(long bufferPointer, long pos, boolean bigEndian) {
        return bigEndian ? readLongB(bufferPointer, pos) : readLongL(bufferPointer, pos);
    }

    public static long readLongB(long bufferPointer, long pos) {
        return EndiannessUtil.readLongB(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static long readLongL(long bufferPointer, long pos) {
        return EndiannessUtil.readLongL(NATIVE_ACCESS, null, bufferPointer + pos);
    }

    public static void writeLong(long bufferPointer, long pos, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongB(bufferPointer, pos, v);
        } else {
            writeLongL(bufferPointer, pos, v);
        }
    }

    public static void writeLongB(long bufferPointer, long pos, long v) {
        EndiannessUtil.writeLongB(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }

    public static void writeLongL(long bufferPointer, long pos, long v) {
        EndiannessUtil.writeLongL(NATIVE_ACCESS, null, bufferPointer + pos, v);
    }



    public static int writeUtf8Char(long bufferPointer, long pos, int c) {
        return EndiannessUtil.writeUtf8Char(NATIVE_ACCESS, null, bufferPointer + pos, c);
    }

    public static char readUtf8Char(OffHeapDataInput in, byte firstByte) throws IOException {
        return EndiannessUtil.readUtf8Char(in, firstByte);
    }
}

