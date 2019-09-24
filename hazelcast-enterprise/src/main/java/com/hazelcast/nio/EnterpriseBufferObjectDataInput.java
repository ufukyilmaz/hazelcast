package com.hazelcast.nio;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.BufferObjectDataInput;

import java.io.IOException;

/**
 * Contract point for {@link BufferObjectDataInput} implementations on enterprise.
 *
 * @see com.hazelcast.nio.EnterpriseObjectDataInput
 * @see BufferObjectDataInput
 */
public interface EnterpriseBufferObjectDataInput
        extends EnterpriseObjectDataInput, BufferObjectDataInput {

    /**
     * Copies the input data to given {@code memoryBlock}.
     *
     * @param memoryBlock the {@link com.hazelcast.internal.memory.MemoryBlock} to copy input data to there
     * @param offset      the offset at the given {@code memoryBlock}> to copy input data to there
     * @param length      the length of the input data to be copied to given {@code memoryBlock}
     */
    void copyToMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;
}
