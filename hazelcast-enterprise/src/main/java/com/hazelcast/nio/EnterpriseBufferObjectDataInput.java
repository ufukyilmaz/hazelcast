package com.hazelcast.nio;

import com.hazelcast.memory.MemoryBlock;

import java.io.IOException;

public interface EnterpriseBufferObjectDataInput extends EnterpriseObjectDataInput, BufferObjectDataInput {

    void copyToMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;

}
