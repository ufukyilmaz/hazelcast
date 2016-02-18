package com.hazelcast.memory;

/**
 * Memory Manager allocates/frees memory blocks from/to OS like C malloc()/free()
 *
 * Additionally a Memory Manager implementation may keep created memory blocks in its own pool
 * instead of giving back to OS.
 *
 * @author mdogan 03/12/13
 */
public interface MemoryManager extends MemoryAllocator {

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
     * @param size size of memory block
     */
    void free(long address, long size);

    /**
     * Unwraps internal system memory allocator to allocate memory directly from system
     * instead of pooling.
     *
     * @return unwrapped memory allocator
     */
    MemoryAllocator unwrapMemoryAllocator();

    /**
     * Compacts the memory region
     */
    void compact();

    /**
     * Destroys this Memory Manager and releases all allocated resources.
     */
    void destroy();

    /**
     * Gets the destroyed state of this memory manager.
     *
     * @return <tt>true</tt> if this memory manager is destroyed, <tt>false</tt> otherwise.
     */
    boolean isDestroyed();

    /**
     * Gets the memory statistics of this memory manager.
     *
     * @return the memory statistics
     */
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
     * <p/>
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
     * <p/>
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
     * <p/>
     * Use case for this sequence is to avoid ABA problem by using it with the address
     * to create a "safe pointer" {@code (address, sequence)} to a memory block.
     *
     * @return new sequence
     */
    long newSequence();

}
