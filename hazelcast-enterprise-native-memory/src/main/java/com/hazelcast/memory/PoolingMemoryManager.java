package com.hazelcast.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.util.QuickMath;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_METADATA_SPACE_PERCENTAGE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_PAGE_SIZE;
import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;
import static com.hazelcast.util.QuickMath.isPowerOfTwo;

/**
 * {@link MemoryManager} implementation which allocates native memory in fixed-size pages and
 * then internally manages application-level allocation within the pages.
 *
 * @author mdogan 17/12/13
 */
public class PoolingMemoryManager implements MemoryManager, GarbageCollectable {

    static final int MIN_MIN_BLOCK_SIZE = 1 << 3;
    static final int MAX_PAGE_SIZE = 1 << 30;

    private final LibMalloc malloc = new UnsafeMalloc();
    private final PooledNativeMemoryStats memoryStats;
    private final GlobalPoolingMemoryManager globalMemoryManager;
    private final Map<Thread, MemoryManager> threadLocalManagers
            = new ConcurrentHashMap<Thread, MemoryManager>(32, .75f, 1);

    private final SimpleGarbageCollector gc = new SimpleGarbageCollector();

    public PoolingMemoryManager(MemorySize cap) {
        this(cap, DEFAULT_MIN_BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_METADATA_SPACE_PERCENTAGE);
    }

    public PoolingMemoryManager(MemorySize size, int minBlockSize, int pageSize) {
        this(size, minBlockSize, pageSize, DEFAULT_METADATA_SPACE_PERCENTAGE);
    }

    public PoolingMemoryManager(MemorySize cap, int minBlockSize, int pageSize, float metadataSpacePercentage) {
        long totalSize = cap.bytes();
        if (totalSize <= 0) {
            throw new IllegalArgumentException("Capacity must be positive!");
        }

        checkFreeMemory(totalSize);
        checkBlockAndPageSize(minBlockSize, pageSize);
        long maxMetadata = (long) (totalSize * metadataSpacePercentage / 100);
        long maxNative = QuickMath.normalize(totalSize - maxMetadata, pageSize);

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
            throw new IllegalArgumentException(
                    "Minimum block size must be greater than or equal to: " + MIN_MIN_BLOCK_SIZE);
        }

        if (!isPowerOfTwo(pageSize)) {
            throw new IllegalArgumentException("Page size must be power of two! -> " + pageSize);
        }

        if (pageSize < minBlockSize) {
            throw new IllegalArgumentException("Page size must be bigger than min block size! "
                    + pageSize + " VS " + minBlockSize);
        }

        if (pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("Page size must be smaller than or equal to: " + MAX_PAGE_SIZE);
        }
    }

    @Override
    public long allocate(long size) {
        MemoryManager manager = getMemoryManager();
        return manager.allocate(size);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        MemoryManager manager = getMemoryManager();
        return manager.reallocate(address, currentSize, newSize);
    }

    @Override
    public void free(long address, long size) {
        MemoryManager manager = getMemoryManager();
        manager.free(address, size);
    }

    @Override
    public void compact() {
        MemoryManager manager = getMemoryManager();
        manager.compact();
    }

    @Override
    public long getUsableSize(long address) {
        MemoryManager manager = getMemoryManager();
        return manager.getUsableSize(address);
    }

    @Override
    public long validateAndGetUsableSize(long address) {
        MemoryManager manager = getMemoryManager();
        return manager.validateAndGetUsableSize(address);
    }

    @Override
    public long getAllocatedSize(long address) {
        MemoryManager manager = getMemoryManager();
        return manager.getAllocatedSize(address);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        MemoryManager manager = getMemoryManager();
        return manager.validateAndGetAllocatedSize(address);
    }

    @Override
    public long newSequence() {
        MemoryManager manager = getMemoryManager();
        return manager.newSequence();
    }

    public int getHeaderSize() {
        AbstractPoolingMemoryManager manager = (AbstractPoolingMemoryManager) getMemoryManager();
        return manager.getHeaderSize();
    }

    public GlobalPoolingMemoryManager getGlobalMemoryManager() {
        return globalMemoryManager;
    }

    public MemoryManager getMemoryManager() {
        Thread current = Thread.currentThread();
        MemoryManager pool = threadLocalManagers.get(current);
        if (pool == null) {
            pool = globalMemoryManager;
        }
        return pool;
    }

    @Override
    public void destroy() {
        gc.abort();

        Collection<MemoryManager> managers = threadLocalManagers.values();
        if (!managers.isEmpty()) {
            Iterator<MemoryManager> iterator = managers.iterator();
            while (iterator.hasNext()) {
                MemoryManager pool = iterator.next();
                iterator.remove();
                destroyPool(pool);
            }
        }
        threadLocalManagers.clear();
        destroyPool(globalMemoryManager);
        memoryStats.resetUsedNativeMemory();
    }

    @Override
    public boolean isDestroyed() {
        MemoryManager manager = getMemoryManager();
        return manager == null || manager.isDestroyed();
    }

    private static void destroyPool(MemoryManager pool) {
        try {
            pool.destroy();
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
        MemoryManager pool = threadLocalManagers.remove(thread);
        if (pool != null) {
            destroyPool(pool);
        }
        gc();
    }

    @Override
    public final void gc() {
        if (!threadLocalManagers.isEmpty()) {
            Iterator<Map.Entry<Thread, MemoryManager>> iter = threadLocalManagers.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Thread, MemoryManager> next = iter.next();
                Thread t = next.getKey();
                if (!t.isAlive()) {
                    iter.remove();
                    MemoryManager pool = next.getValue();
                    destroyPool(pool);
                }
            }
        }
    }

    @Override
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return globalMemoryManager.unwrapMemoryAllocator();
    }

    @Override
    public String toString() {
        return "PoolingMemoryManager{globalMemoryManager=" + globalMemoryManager + '}';
    }
}
