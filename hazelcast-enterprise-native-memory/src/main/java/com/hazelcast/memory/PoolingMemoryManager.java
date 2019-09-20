package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.QuickMath;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_METADATA_SPACE_PERCENTAGE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_PAGE_SIZE;
import static com.hazelcast.internal.util.QuickMath.isPowerOfTwo;

/**
 * Implements a hierachical memory allocation scheme:
 * <ol><li>
 *     Tier-1 is the platform-native memory allocator. It is used to allocate <i>pages</i>
 *     managed by Tier-2. Also, larger blocks will be allocated directly from this tier.
 *     Size of the page is configurable and the default is
 *     {@value com.hazelcast.config.NativeMemoryConfig#DEFAULT_PAGE_SIZE} bytes.
 *     </li><li>
 *     Tier-2 is the custom allocator which manages memory within the pages using the
 *     <i>buddy allocation policy</i>. Size of the allocated blocks is always of a power of two and the base
 *     address of each block is 8-byte aligned. The size of the smallest block that will be allocated is
 *     configurable (but never below 8 bytes) and the default is
 *     {@value com.hazelcast.config.NativeMemoryConfig#DEFAULT_MIN_BLOCK_SIZE} bytes. The client can request to
 *     allocate a block of any size; this will result in the allocation of the next-larger power-of-two block which
 *     will be only partially utilized by the client (resulting in <i>internal memory fragmentation</i>).
 * </li></ol>
 * This class is only the top-level coordinator of actual memory managers, which come in two flavors:
 * <ol><li>
 *     <i>Thread-local</i>: allocates pages for exclusive use within a single thread. Eliminates the issue
 *     of concurrent contention on allocation/deallocation methods, but prevents the transfer of block ownership
 *     to another thread. A block must be {@code free}d by the thread that {@code allocate}d it.
 * </li><li>
 *     <i>Global</i>: serves allocation requests done on any thread which was not previously registered for
 *     thread-local allocation. Concurrent requests will have to contend for access, hurting performance.
 * </li></ol>
 * Which manager will serve a particular request is decided internally by this class: if the current thread was
 * previously registered with this manager by calling {@link #registerThread(Thread)}, the thread-local manager
 * dedicated to that thread will be looked up (or created); otherwise the request will be forwarded to the
 * global manager.
 */
public class PoolingMemoryManager implements HazelcastMemoryManager, GarbageCollectable, MetricsProvider {

    @SuppressWarnings("checkstyle:magicnumber")
    static final int MIN_MIN_BLOCK_SIZE = 1 << 3;
    @SuppressWarnings("checkstyle:magicnumber")
    static final int MAX_PAGE_SIZE = 1 << 30;

    private static final int PERCENTAGE_FACTOR = 100;
    private static final FreeMemoryChecker DEFAULT_FREE_MEMORY_CHECKER = new FreeMemoryChecker();
    private static final LibMallocFactory DEFAULT_LIB_MALLOC_FACTORY = new UnsafeMallocFactory(DEFAULT_FREE_MEMORY_CHECKER);

    private final LibMalloc malloc;
    private final PooledNativeMemoryStats memoryStats;
    private final GlobalPoolingMemoryManager globalMemoryManager;
    private final Map<Thread, HazelcastMemoryManager> threadLocalManagers
            = new ConcurrentHashMap<Thread, HazelcastMemoryManager>(32, .75f, 1);

    private final SimpleGarbageCollector gc = new SimpleGarbageCollector();

    public PoolingMemoryManager(MemorySize cap) {
        this(cap, DEFAULT_MIN_BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_METADATA_SPACE_PERCENTAGE,
                DEFAULT_LIB_MALLOC_FACTORY);
    }

    public PoolingMemoryManager(MemorySize size, int minBlockSize, int pageSize) {
        this(size, minBlockSize, pageSize, DEFAULT_METADATA_SPACE_PERCENTAGE,
                DEFAULT_LIB_MALLOC_FACTORY);
    }

    public PoolingMemoryManager(MemorySize size, int minBlockSize, int pageSize, float metadataSpacePercentage) {
        this(size, minBlockSize, pageSize, metadataSpacePercentage, DEFAULT_LIB_MALLOC_FACTORY);
    }

    public PoolingMemoryManager(MemorySize size, int minBlockSize, int pageSize,  LibMallocFactory libMallocFactory) {
        this(size, minBlockSize, pageSize, DEFAULT_METADATA_SPACE_PERCENTAGE,
                libMallocFactory);
    }

    public PoolingMemoryManager(MemorySize cap, int minBlockSize,
                                int pageSize, float metadataSpacePercentage,
                                LibMallocFactory libMallocFactory) {
        long totalSize = cap.bytes();
        if (totalSize <= 0) {
            throw new IllegalArgumentException("Capacity must be positive!");
        }

        checkBlockAndPageSize(minBlockSize, pageSize);
        long maxMetadata = (long) (totalSize * metadataSpacePercentage / PERCENTAGE_FACTOR);
        long maxNative = QuickMath.normalize(totalSize - maxMetadata, pageSize);
        malloc = libMallocFactory.create(totalSize);

        memoryStats = new PooledNativeMemoryStats(maxNative, maxMetadata);
        globalMemoryManager = new GlobalPoolingMemoryManager(minBlockSize, pageSize, malloc, memoryStats, gc);

        gc.registerGarbageCollectable(this);
        gc.start();
    }

    static void checkBlockAndPageSize(int minBlockSize, int pageSize) {
        if (!isPowerOfTwo(minBlockSize)) {
            throw new IllegalArgumentException("Minimum block size must be power of two! -> " + minBlockSize);
        }

        if (minBlockSize < MIN_MIN_BLOCK_SIZE) {
            throw new IllegalArgumentException("Minimum block size must be greater than or equal to: " + MIN_MIN_BLOCK_SIZE);
        }

        if (!isPowerOfTwo(pageSize)) {
            throw new IllegalArgumentException("Page size must be power of two! -> " + pageSize);
        }

        if (pageSize < minBlockSize) {
            throw new IllegalArgumentException("Page size must be bigger than min block size! " + pageSize
                    + " vs. " + minBlockSize);
        }

        if (pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("Page size must be smaller than or equal to: " + MAX_PAGE_SIZE);
        }
    }

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
    @Override
    public long allocate(long size) {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.allocate(size);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.reallocate(address, currentSize, newSize);
    }

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
    @Override
    public void free(long address, long size) {
        HazelcastMemoryManager manager = getMemoryManager();
        manager.free(address, size);
    }

    @Override
    public void compact() {
        HazelcastMemoryManager manager = getMemoryManager();
        manager.compact();
    }

    @Override
    public long getUsableSize(long address) {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.getUsableSize(address);
    }

    @Override
    public long validateAndGetUsableSize(long address) {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.validateAndGetUsableSize(address);
    }

    @Override
    public long getAllocatedSize(long address) {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.getAllocatedSize(address);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.validateAndGetAllocatedSize(address);
    }

    @Override
    public long newSequence() {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager.newSequence();
    }

    public int getHeaderSize() {
        AbstractPoolingMemoryManager manager = (AbstractPoolingMemoryManager) getMemoryManager();
        return manager.headerSize();
    }

    public GlobalPoolingMemoryManager getGlobalMemoryManager() {
        return globalMemoryManager;
    }

    public HazelcastMemoryManager getMemoryManager() {
        Thread current = Thread.currentThread();
        HazelcastMemoryManager pool = threadLocalManagers.get(current);
        if (pool == null) {
            pool = globalMemoryManager;
        }
        return pool;
    }

    @Override
    public void dispose() {
        gc.abort();

        Collection<HazelcastMemoryManager> managers = threadLocalManagers.values();
        if (!managers.isEmpty()) {
            Iterator<HazelcastMemoryManager> iterator = managers.iterator();
            while (iterator.hasNext()) {
                HazelcastMemoryManager pool = iterator.next();
                iterator.remove();
                destroyPool(pool);
            }
        }
        threadLocalManagers.clear();
        destroyPool(globalMemoryManager);
        malloc.dispose();
        memoryStats.resetUsedNativeMemory();
    }

    @Override
    public boolean isDisposed() {
        HazelcastMemoryManager manager = getMemoryManager();
        return manager == null || manager.isDisposed();
    }

    private static void destroyPool(HazelcastMemoryManager pool) {
        try {
            pool.dispose();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void registerThread(Thread thread) {
        if (threadLocalManagers.containsKey(thread)) {
            throw new IllegalArgumentException();
        }
        gc();
        threadLocalManagers.put(thread, newThreadLocalPoolingMemoryManager(
                globalMemoryManager.minBlockSize, globalMemoryManager.pageSize, malloc, memoryStats));
    }

    protected ThreadLocalPoolingMemoryManager newThreadLocalPoolingMemoryManager(
            int minBlockSize, int pageSize, LibMalloc malloc, PooledNativeMemoryStats stats
    ) {
        return new ThreadLocalPoolingMemoryManager(minBlockSize, pageSize, malloc, stats);
    }

    public void deregisterThread(Thread thread) {
        HazelcastMemoryManager pool = threadLocalManagers.remove(thread);
        if (pool != null) {
            destroyPool(pool);
        }
        gc();
    }

    @Override
    public final void gc() {
        if (!threadLocalManagers.isEmpty()) {
            Iterator<Map.Entry<Thread, HazelcastMemoryManager>> iter = threadLocalManagers.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Thread, HazelcastMemoryManager> next = iter.next();
                Thread t = next.getKey();
                if (!t.isAlive()) {
                    iter.remove();
                    HazelcastMemoryManager pool = next.getValue();
                    destroyPool(pool);
                }
            }
        }
    }

    @Override
    public MemoryAllocator getSystemAllocator() {
        return globalMemoryManager.getSystemAllocator();
    }

    @Override
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public String toString() {
        return "PoolingMemoryManager{globalMemoryManager=" + globalMemoryManager + '}';
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(memoryStats, "memorymanager.stats");
    }
}
