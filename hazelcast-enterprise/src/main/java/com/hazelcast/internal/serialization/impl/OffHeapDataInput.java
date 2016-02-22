package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays of primitive types which are located in off-heap
 * Let us to work with long pointers
 */
public interface OffHeapDataInput extends ObjectDataInput {

    int read() throws IOException;

    int read(long position) throws IOException;

    boolean readBoolean(long position) throws IOException;

    byte readByte(long position) throws IOException;

    char readChar(long position) throws IOException;

    double readDouble(int position) throws IOException;

    double readDouble(ByteOrder byteOrder) throws IOException;

    double readDouble(long position, ByteOrder byteOrder) throws IOException;

    float readFloat(int position) throws IOException;

    float readFloat(ByteOrder byteOrder) throws IOException;

    float readFloat(int position, ByteOrder byteOrder) throws IOException;

    int readInt(ByteOrder byteOrder) throws IOException;

    int readInt(long position, ByteOrder byteOrder) throws IOException;

    long readLong(ByteOrder byteOrder) throws IOException;

    long readLong(long position, ByteOrder byteOrder) throws IOException;

    short readShort(long position) throws IOException;

    short readShort(ByteOrder byteOrder) throws IOException;

    short readShort(long position, ByteOrder byteOrder) throws IOException;

    long position();

    void position(long newPos);

    long available();

    void reset(long pointer, long size);

    void clear();

    void close();
}
