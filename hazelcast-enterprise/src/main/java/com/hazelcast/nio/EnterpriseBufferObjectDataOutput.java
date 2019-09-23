package com.hazelcast.nio;

import com.hazelcast.internal.memory.MemoryBlock;

import java.io.IOException;

/**
 * Contract point for {@link com.hazelcast.nio.BufferObjectDataOutput} implementations on enterprise.
 *
 * @see com.hazelcast.nio.EnterpriseObjectDataOutput
 * @see com.hazelcast.nio.BufferObjectDataOutput
 */
public interface EnterpriseBufferObjectDataOutput
        extends EnterpriseObjectDataOutput, BufferObjectDataOutput {

    /**
     * Copies the input data from given {@code memoryBlock}.
     *
     * @param memoryBlock the {@link com.hazelcast.internal.memory.MemoryBlock} to copy input data to here
     * @param offset      the offset at the given {@code memoryBlock} to copy input data to here
     * @param length      the length of the input data to be copied from given {@code memoryBlock}
     */
    void copyFromMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;

    /**
     * Copies the input data to given {@code memoryBlock}.
     *
     * @param memoryBlock the {@link com.hazelcast.internal.memory.MemoryBlock} to copy input data to there
     * @param offset      the offset at the given {@code memoryBlock} to copy input data to there
     * @param length      the length of the input data to be copied to given {@code memoryBlock}
     */
    void copyToMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;
}
