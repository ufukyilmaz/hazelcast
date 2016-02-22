package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides methods which let to access to serialized data located in off-heap
 * <p/>
 * It works like a factory for the memory-blocks creating and returning pointer and sizes
 * It doesn't release memory, memory-releasing is responsibility of the external environment
 */
public interface OffHeapDataOutput extends ObjectDataOutput {

    /**
     * @return address of the dataBlock in off-heap
     */
    long getPointer();

    /**
     * @return written bytes into the dataBlock
     */
    long getWrittenSize();

    /**
     * @return real allocated size in bytes of the dataBlock
     */
    long getAllocatedSize();

    void write(long position, int b);

    void writeBoolean(long position, boolean v) throws IOException;

    void writeZeroBytes(int count);

    void writeByte(long position, int v) throws IOException;

    void writeChar(long position, int v) throws IOException;

    void writeDouble(long position, double v) throws IOException;

    void writeDouble(double v, ByteOrder byteOrder) throws IOException;

    void writeDouble(long position, double v, ByteOrder byteOrder) throws IOException;

    void writeFloat(long position, float v) throws IOException;

    void writeFloat(float v, ByteOrder byteOrder) throws IOException;

    void writeFloat(long position, float v, ByteOrder byteOrder) throws IOException;

    void writeInt(long position, int v) throws IOException;

    void writeInt(int v, ByteOrder byteOrder) throws IOException;

    void writeInt(long position, int v, ByteOrder byteOrder) throws IOException;

    void writeLong(long position, long v) throws IOException;

    void writeLong(long v, ByteOrder byteOrder) throws IOException;

    void writeLong(long position, long v, ByteOrder byteOrder) throws IOException;

    void writeShort(long position, int v) throws IOException;

    void writeShort(int v, ByteOrder byteOrder) throws IOException;

    void writeShort(long position, int v, ByteOrder byteOrder) throws IOException;

    long position();

    void position(long newPos);

    void clear();

    void close();
}
