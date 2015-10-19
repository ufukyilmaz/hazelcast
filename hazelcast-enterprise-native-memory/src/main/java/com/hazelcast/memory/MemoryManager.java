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

    int SIZE_INVALID = -1;

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
     * Returns true if this MemoryManager is destroyed, false otherwise.
     * @return true if this MemoryManager is destroyed, false otherwise.
     */
    boolean isDestroyed();

    /**
     * @return memory statistics
     */
    MemoryStats getMemoryStats();

    /**
     * Returns page id for given address
     *
     * @param address address of memory block
     * @return page id
     */
    long getPage(long address);

    int getSize(long address);

}
