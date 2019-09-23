package com.hazelcast.internal.memory;

import com.hazelcast.memory.NativeOutOfMemoryError;

/**
 * Specialization of {@link MemoryAllocator} to CPU's native address space.
 * Also includes methods specific to Hazelcast's pooling memory managers.
 */
public interface HazelcastMemoryManager extends MemoryAllocator {

    /**
     * Indicates that size of a memory block is not known by this memory manager.
     */
    long SIZE_INVALID = -1;

    /**
     * Allocates memory from an internal memory pool or falls back to OS
     * if not enough memory available in pool.
     * Content of the memory block will be initialized to zero.
     *
     * <p>
     * Complement of {@link #free(long, long)}.
     * Memory allocated by this method should be freed using
     * {@link #free(long, long)}
     *
     * @param size of requested memory block
     * @return address of memory block
     * @throws NativeOutOfMemoryError if not enough memory is available
     */
    long allocate(long size);

    /**
     * Gives allocated memory block back to internal pool or to OS
     * if pool is over capacity.
     *
     * <p>
     * Complement of {@link #allocate(long)}.
     * Only memory allocated by {@link #allocate(long)} can be
     * freed using this method.
     *
     * @param address address of memory block
     * @param size    size of memory block
     */
    void free(long address, long size);

    /**
     * @return the metadata memory allocator.
     */
    MemoryAllocator getSystemAllocator();

    /**
     * Compacts the memory region.
     */
    void compact();

    /**
     * Gets the disposed state of this memory manager.
     *
     * @return {@code true} if this memory manager is destroyed, {@code false} otherwise
     */
    boolean isDisposed();

    MemoryStats getMemoryStats();

    /**
     * Returns size of memory block as if it belongs to and is allocated by this memory manager.
     * If memory block is not known by this memory manager, {@link #SIZE_INVALID} is returned.
     *
     * @param address address of memory block
     * @return size of memory block or {@link #SIZE_INVALID} if it's unknown
     */
    long getUsableSize(long address);

    /**
     * Returns size of memory block if and only if it belongs to and is allocated by this memory manager.
     * Otherwise, if memory block is not valid anymore or it is not known by this memory manager,
     * {@link #SIZE_INVALID} is returned.
     *
     * @param address address of memory block
     * @return size of memory block or {@link #SIZE_INVALID} if it's unknown
     */
    long validateAndGetUsableSize(long address);

    /**
     * Returns allocated size of memory block as if it belongs to and is allocated by this memory manager.
     * If memory block is not known by this memory manager, {@link #SIZE_INVALID} is returned.
     * <p>
     * Allocated size includes usable memory size plus any header
     * or metadata region size reserved by memory manager.
     *
     * @param address address of memory block
     * @return size of memory block or {@link #SIZE_INVALID} if it's unknown
     * @see #getUsableSize(long)
     */
    long getAllocatedSize(long address);

    /**
     * Returns allocated size of memory block if and only if it belongs to and is allocated by this memory manager.
     * Otherwise, if memory block is not valid anymore or it is not known by this memory manager,
     * {@link #SIZE_INVALID} is returned.
     * <p>
     * Allocated size includes usable memory size plus any header
     * or metadata region size reserved by memory manager.
     *
     * @param address address of memory block
     * @return size of memory block or {@link #SIZE_INVALID} if it's unknown
     * @see #validateAndGetUsableSize(long)
     */
    long validateAndGetAllocatedSize(long address);

    /**
     * Creates a new sequence. This sequence is guaranteed to be atomically incremental.
     * <p>
     * Use case for this sequence is to avoid ABA problem by using it with the address
     * to create a "safe pointer" {@code (address, sequence)} to a memory block.
     *
     * @return new sequence
     */
    long newSequence();
}
