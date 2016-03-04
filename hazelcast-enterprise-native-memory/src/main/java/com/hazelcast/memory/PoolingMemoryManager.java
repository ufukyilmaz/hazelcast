package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAccessor;
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
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.memory.FreeMemoryChecker.checkFreeMemory;
import static com.hazelcast.util.QuickMath.isPowerOfTwo;

/**
 * {@link JvmMemoryManager} implementation which allocates native memory in fixed-size pages and
 * then internally manages application-level allocation within the pages.
 */
public class PoolingMemoryManager implements JvmMemoryManager, MemoryAllocator, GarbageCollectable {

    @SuppressWarnings("checkstyle:magicnumber")
    static final int MIN_MIN_BLOCK_SIZE = 1 << 3;
    @SuppressWarnings("checkstyle:magicnumber")
    static final int MAX_PAGE_SIZE = 1 << 30;

    private static final int PERCENTAGE_FACTOR = 100;

    private final LibMalloc malloc = new UnsafeMalloc();
    private final PooledNativeMemoryStats memoryStats;
    private final GlobalPoolingMemoryManager globalMemoryManager;
    private final Map<Thread, JvmMemoryManager> threadLocalManagers
            = new ConcurrentHashMap<Thread, JvmMemoryManager>(32, .75f, 1);

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
        long maxMetadata = (long) (totalSize * metadataSpacePercentage / PERCENTAGE_FACTOR);
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

    @Override
    public long allocate(long size) {
        JvmMemoryManager manager = getMemoryManager();
        return manager.allocate(size);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        JvmMemoryManager manager = getMemoryManager();
        return manager.getAllocator().reallocate(address, currentSize, newSize);
    }

    @Override
    public void free(long address, long size) {
        JvmMemoryManager manager = getMemoryManager();
        manager.free(address, size);
    }

    @Override
    public void compact() {
        JvmMemoryManager manager = getMemoryManager();
        manager.compact();
    }

    @Override
    public long getUsableSize(long address) {
        JvmMemoryManager manager = getMemoryManager();
        return manager.getUsableSize(address);
    }

    @Override
    public long validateAndGetUsableSize(long address) {
        JvmMemoryManager manager = getMemoryManager();
        return manager.validateAndGetUsableSize(address);
    }

    @Override
    public long getAllocatedSize(long address) {
        JvmMemoryManager manager = getMemoryManager();
        return manager.getAllocatedSize(address);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        JvmMemoryManager manager = getMemoryManager();
        return manager.validateAndGetAllocatedSize(address);
    }

    @Override
    public long newSequence() {
        JvmMemoryManager manager = getMemoryManager();
        return manager.newSequence();
    }

    public int getHeaderSize() {
        AbstractPoolingMemoryManager manager = (AbstractPoolingMemoryManager) getMemoryManager();
        return manager.getHeaderSize();
    }

    public GlobalPoolingMemoryManager getGlobalMemoryManager() {
        return globalMemoryManager;
    }

    public JvmMemoryManager getMemoryManager() {
        Thread current = Thread.currentThread();
        JvmMemoryManager pool = threadLocalManagers.get(current);
        if (pool == null) {
            pool = globalMemoryManager;
        }
        return pool;
    }

    @Override
    public void dispose() {
        gc.abort();

        Collection<JvmMemoryManager> managers = threadLocalManagers.values();
        if (!managers.isEmpty()) {
            Iterator<JvmMemoryManager> iterator = managers.iterator();
            while (iterator.hasNext()) {
                JvmMemoryManager pool = iterator.next();
                iterator.remove();
                destroyPool(pool);
            }
        }
        threadLocalManagers.clear();
        destroyPool(globalMemoryManager);
        memoryStats.resetUsedNativeMemory();
    }

    @Override
    public boolean isDisposed() {
        JvmMemoryManager manager = getMemoryManager();
        return manager == null || manager.isDisposed();
    }

    private static void destroyPool(JvmMemoryManager pool) {
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
        JvmMemoryManager pool = threadLocalManagers.remove(thread);
        if (pool != null) {
            destroyPool(pool);
        }
        gc();
    }

    @Override
    public final void gc() {
        if (!threadLocalManagers.isEmpty()) {
            Iterator<Map.Entry<Thread, JvmMemoryManager>> iter = threadLocalManagers.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Thread, JvmMemoryManager> next = iter.next();
                Thread t = next.getKey();
                if (!t.isAlive()) {
                    iter.remove();
                    JvmMemoryManager pool = next.getValue();
                    destroyPool(pool);
                }
            }
        }
    }

    @Override
    public JVMMemoryStats getMemoryStats() {
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

    @Override
    public MemoryAllocator getAllocator() {
        return this;
    }

    @Override
    public MemoryAccessor getAccessor() {
        return MEM;
    }
}
