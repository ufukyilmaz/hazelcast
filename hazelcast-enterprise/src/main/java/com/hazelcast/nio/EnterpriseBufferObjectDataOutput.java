package com.hazelcast.nio;

import com.hazelcast.memory.MemoryBlock;

import java.io.IOException;

public interface EnterpriseBufferObjectDataOutput extends BufferObjectDataOutput {

    void copyFromMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;

    void copyToMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;

}
