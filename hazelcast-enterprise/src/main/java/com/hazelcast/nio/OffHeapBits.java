package com.hazelcast.nio;

import com.hazelcast.internal.serialization.impl.OffHeapDataInput;

import java.io.IOException;
import java.io.UTFDataFormatException;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;

public final class OffHeapBits {
    private OffHeapBits() {
    }

    public static char readChar(long bufferPointer, long pos, boolean bigEndian) {
        return bigEndian ? readCharB(bufferPointer, pos) : readCharL(bufferPointer, pos);
    }

    public static char readCharB(long bufferPointer, long pos) {
        int byte1 = MEM.getByte(bufferPointer + pos) & 0xFF;
        int byte0 = MEM.getByte(bufferPointer + pos + 1) & 0xFF;
        return (char) ((byte1 << 8) + byte0);
    }

    public static char readCharL(long bufferPointer, long pos) {
        int byte1 = MEM.getByte(bufferPointer + pos) & 0xFF;
        int byte0 = MEM.getByte(bufferPointer + pos + 1) & 0xFF;
        return (char) ((byte0 << 8) + byte1);
    }

    public static void writeChar(long bufferPointer, long pos, char v, boolean bigEndian) {
        if (bigEndian) {
            writeCharB(bufferPointer, pos, v);
        } else {
            writeCharL(bufferPointer, pos, v);
        }
    }

    public static void writeCharB(long bufferPointer, long pos, char v) {
        MEM.putByte(bufferPointer + pos, (byte) ((v >>> 8) & 0xFF));
        MEM.putByte(bufferPointer + pos + 1, (byte) ((v) & 0xFF));
    }

    public static void writeCharL(long bufferPointer, long pos, char v) {
        MEM.putByte(bufferPointer + pos, (byte) ((v) & 0xFF));
        MEM.putByte(bufferPointer + pos + 1, (byte) ((v >>> 8) & 0xFF));
    }

    public static short readShort(long bufferPointer, long pos, boolean bigEndian) {
        return bigEndian ? readShortB(bufferPointer, pos) : readShortL(bufferPointer, pos);
    }

    public static short readShortB(long bufferPointer, long pos) {
        int byte1 = MEM.getByte(bufferPointer + pos) & 0xFF;
        int byte0 = MEM.getByte(bufferPointer + pos + 1) & 0xFF;
        return (short) ((byte1 << 8) + byte0);
    }

    public static short readShortL(long bufferPointer, long pos) {
        int byte1 = MEM.getByte(bufferPointer + pos) & 0xFF;
        int byte0 = MEM.getByte(bufferPointer + pos + 1) & 0xFF;
        return (short) ((byte0 << 8) + byte1);
    }

    public static void writeShort(long bufferPointer, long pos, short v, boolean bigEndian) {
        if (bigEndian) {
            writeShortB(bufferPointer, pos, v);
        } else {
            writeShortL(bufferPointer, pos, v);
        }
    }

    public static void writeShortB(long bufferPointer, long pos, short v) {
        MEM.putByte(bufferPointer + pos, (byte) ((v >>> 8) & 0xFF));
        MEM.putByte(bufferPointer + pos + 1, (byte) ((v) & 0xFF));
    }

    public static void writeShortL(long bufferPointer, long pos, short v) {
        MEM.putByte(bufferPointer + pos, (byte) ((v) & 0xFF));
        MEM.putByte(bufferPointer + pos + 1, (byte) ((v >>> 8) & 0xFF));
    }

    public static int readInt(long bufferPointer, long pos, boolean bigEndian) {
        if (bigEndian) {
            return readIntB(bufferPointer, pos);
        } else {
            return readIntL(bufferPointer, pos);
        }
    }

    public static int readIntB(long bufferPointer, long pos) {
        int byte3 = (MEM.getByte(bufferPointer + pos) & 0xFF) << 24;
        int byte2 = (MEM.getByte(bufferPointer + pos + 1) & 0xFF) << 16;
        int byte1 = (MEM.getByte(bufferPointer + pos + 2) & 0xFF) << 8;
        int byte0 = MEM.getByte(bufferPointer + pos + 3) & 0xFF;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static int readIntL(long bufferPointer, long pos) {
        int byte3 = MEM.getByte(bufferPointer + pos) & 0xFF;
        int byte2 = (MEM.getByte(bufferPointer + pos + 1) & 0xFF) << 8;
        int byte1 = (MEM.getByte(bufferPointer + pos + 2) & 0xFF) << 16;
        int byte0 = (MEM.getByte(bufferPointer + pos + 3) & 0xFF) << 24;
        return byte3 + byte2 + byte1 + byte0;
    }

    public static void writeInt(long bufferPointer, long pos, int v, boolean bigEndian) {
        if (bigEndian) {
            writeIntB(bufferPointer, pos, v);
        } else {
            writeIntL(bufferPointer, pos, v);
        }
    }

    public static void writeIntB(long bufferPointer, long pos, int v) {
        MEM.putByte(bufferPointer + pos, (byte) ((v >>> 24) & 0xFF));
        MEM.putByte(bufferPointer + pos + 1, (byte) ((v >>> 16) & 0xFF));
        MEM.putByte(bufferPointer + pos + 2, (byte) ((v >>> 8) & 0xFF));
        MEM.putByte(bufferPointer + pos + 3, (byte) ((v) & 0xFF));
    }

    public static void writeIntL(long bufferPointer, long pos, int v) {
        MEM.putByte(bufferPointer + pos, (byte) ((v) & 0xFF));
        MEM.putByte(bufferPointer + pos + 1, (byte) ((v >>> 8) & 0xFF));
        MEM.putByte(bufferPointer + pos + 2, (byte) ((v >>> 16) & 0xFF));
        MEM.putByte(bufferPointer + pos + 3, (byte) ((v >>> 24) & 0xFF));
    }

    public static long readLong(long bufferPointer, long pos, boolean bigEndian) {
        if (bigEndian) {
            return readLongB(bufferPointer, pos);
        } else {
            return readLongL(bufferPointer, pos);
        }
    }

    public static long readLongB(long bufferPointer, long pos) {
        long byte7 = (long) MEM.getByte(bufferPointer + pos) << 56;
        long byte6 = (long) (MEM.getByte(bufferPointer + pos + 1) & 0xFF) << 48;
        long byte5 = (long) (MEM.getByte(bufferPointer + pos + 2) & 0xFF) << 40;
        long byte4 = (long) (MEM.getByte(bufferPointer + pos + 3) & 0xFF) << 32;
        long byte3 = (long) (MEM.getByte(bufferPointer + pos + 4) & 0xFF) << 24;
        long byte2 = (long) (MEM.getByte(bufferPointer + pos + 5) & 0xFF) << 16;
        long byte1 = (long) (MEM.getByte(bufferPointer + pos + 6) & 0xFF) << 8;
        long byte0 = (long) (MEM.getByte(bufferPointer + pos + 7) & 0xFF);
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static long readLongL(long bufferPointer, long pos) {
        long byte7 = (long) (MEM.getByte(bufferPointer + pos) & 0xFF);
        long byte6 = (long) (MEM.getByte(bufferPointer + pos + 1) & 0xFF) << 8;
        long byte5 = (long) (MEM.getByte(bufferPointer + pos + 2) & 0xFF) << 16;
        long byte4 = (long) (MEM.getByte(bufferPointer + pos + 3) & 0xFF) << 24;
        long byte3 = (long) (MEM.getByte(bufferPointer + pos + 4) & 0xFF) << 32;
        long byte2 = (long) (MEM.getByte(bufferPointer + pos + 5) & 0xFF) << 40;
        long byte1 = (long) (MEM.getByte(bufferPointer + pos + 6) & 0xFF) << 48;
        long byte0 = (long) (MEM.getByte(bufferPointer + pos + 7) & 0xFF) << 56;
        return byte7 + byte6 + byte5 + byte4 + byte3 + byte2 + byte1 + byte0;
    }

    public static void writeLong(long bufferPointer, long pos, long v, boolean bigEndian) {
        if (bigEndian) {
            writeLongB(bufferPointer, pos, v);
        } else {
            writeLongL(bufferPointer, pos, v);
        }
    }

    public static void writeLongB(long bufferPointer, long pos, long v) {
        MEM.putByte(bufferPointer + pos, (byte) (v >>> 56));
        MEM.putByte(bufferPointer + pos + 1, (byte) (v >>> 48));
        MEM.putByte(bufferPointer + pos + 2, (byte) (v >>> 40));
        MEM.putByte(bufferPointer + pos + 3, (byte) (v >>> 32));
        MEM.putByte(bufferPointer + pos + 4, (byte) (v >>> 24));
        MEM.putByte(bufferPointer + pos + 5, (byte) (v >>> 16));
        MEM.putByte(bufferPointer + pos + 6, (byte) (v >>> 8));
        MEM.putByte(bufferPointer + pos + 7, (byte) (v));
    }

    public static void writeLongL(long bufferPointer, long pos, long v) {
        MEM.putByte(bufferPointer + pos, (byte) (v));
        MEM.putByte(bufferPointer + pos + 1, (byte) (v >>> 8));
        MEM.putByte(bufferPointer + pos + 2, (byte) (v >>> 16));
        MEM.putByte(bufferPointer + pos + 3, (byte) (v >>> 24));
        MEM.putByte(bufferPointer + pos + 4, (byte) (v >>> 32));
        MEM.putByte(bufferPointer + pos + 5, (byte) (v >>> 40));
        MEM.putByte(bufferPointer + pos + 6, (byte) (v >>> 48));
        MEM.putByte(bufferPointer + pos + 7, (byte) (v >>> 56));
    }

    public static int writeUtf8Char(long bufferPointer, long pos, int c) {
        if (c <= 0x007F) {
            MEM.putByte(bufferPointer + pos, (byte) c);
            return 1;
        } else if (c > 0x07FF) {
            MEM.putByte(bufferPointer + pos, (byte) (0xE0 | c >> 12 & 0x0F));
            MEM.putByte(bufferPointer + pos + 1, (byte) (0x80 | c >> 6 & 0x3F));
            MEM.putByte(bufferPointer + pos + 2, (byte) (0x80 | c & 0x3F));
            return 3;
        } else {
            MEM.putByte(bufferPointer + pos, (byte) (0xC0 | c >> 6 & 0x1F));
            MEM.putByte(bufferPointer + pos + 1, (byte) (0x80 | c & 0x3F));
            return 2;
        }
    }

    public static char readUtf8Char(OffHeapDataInput in, byte firstByte)
            throws IOException {
        int b = firstByte & 0xFF;
        switch (b >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) b;
            case 12:
            case 13:
                int first = (b & 0x1F) << 6;
                int second = in.readByte() & 0x3F;
                return (char) (first | second);
            case 14:
                int first2 = (b & 0x0F) << 12;
                int second2 = (in.readByte() & 0x3F) << 6;
                int third2 = in.readByte() & 0x3F;
                return (char) (first2 | second2 | third2);
            default:
                throw new UTFDataFormatException("Malformed byte sequence");
        }
    }
}

