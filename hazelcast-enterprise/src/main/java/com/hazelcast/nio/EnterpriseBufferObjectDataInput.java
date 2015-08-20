package com.hazelcast.nio;

import com.hazelcast.memory.MemoryBlock;

import java.io.IOException;

/**
 * Contract point for {@link com.hazelcast.nio.BufferObjectDataInput} implementations on enterprise.
 *
 * @see com.hazelcast.nio.EnterpriseObjectDataInput
 * @see com.hazelcast.nio.BufferObjectDataInput
 */
public interface EnterpriseBufferObjectDataInput
        extends EnterpriseObjectDataInput, BufferObjectDataInput {

    /**
     * Copies the input data to given <code>memoryBlock</code>.
     *
     * @param memoryBlock   the {@link com.hazelcast.memory.MemoryBlock} to copy input data to there
     * @param offset        the offset at the given <code>memoryBlock</code> to copy input data to there
     * @param length        the length of the input data to be copied to given <code>memoryBlock</code>
     * @throws IOException
     */
    void copyToMemoryBlock(MemoryBlock memoryBlock, int offset, int length) throws IOException;

}
