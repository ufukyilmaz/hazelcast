package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 02/06/14
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemoryManagerTest {

    private final int pageSize = 1 << 22;
    private PoolingMemoryManager memoryManager;

    @Before
    public void init() {
        MemorySize size = new MemorySize(32, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(size, 16, pageSize);
    }

    @After
    public void destroy() {
        memoryManager.destroy();
    }

    @Test
    public void testGlobalCompaction() {
        testCompaction(memoryManager);
    }

    @Test
    public void testThreadLocalCompaction() {
        memoryManager.registerThread(Thread.currentThread());
        testCompaction(memoryManager);
    }

    private void testCompaction(PoolingMemoryManager memoryManager) {
        int maxBlockSize = pageSize / 8;
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        Random rand = new Random();
        Set<MemoryBlock> blocks = new HashSet<MemoryBlock>();
        while (true) {
            try {
                int size = rand.nextInt(maxBlockSize);
                if (size == 0) {
                    continue;
                }
                long address = memoryManager.allocate(size);
                blocks.add(new MemoryBlock(address, size));
            } catch (NativeOutOfMemoryError e) {
                break;
            }
        }
        int minBlockCount = (int) (memoryStats.getMaxNativeMemory() / maxBlockSize);
        assertTrue(blocks.size() >= minBlockCount);
        assertTrue("committed: " + memoryStats.getCommittedHeap() + ", used: " + memoryStats.getUsedNativeMemory(),
                memoryStats.getCommittedHeap() >= memoryStats.getUsedNativeMemory());
        assertTrue("used: " + memoryStats.getUsedNativeMemory() + ", blocks: " + (blocks.size() * maxBlockSize),
                memoryStats.getUsedNativeMemory() <= (maxBlockSize * blocks.size()));

        for (MemoryBlock block : blocks) {
            memoryManager.free(block.address(), block.size());
        }
        assertEquals(0, memoryStats.getUsedNativeMemory());
        memoryManager.compact();

        int headerLength = memoryManager.getHeaderLength();
        for (int i = 7; i >= 0; i--) {
            int size = pageSize / (1 << i) - headerLength;
            long address = memoryManager.allocate(size);
            assertEquals(size + headerLength, memoryStats.getUsedNativeMemory());
            memoryManager.free(address, size);
            assertEquals(0, memoryStats.getUsedNativeMemory());
        }
    }

    @Test
    public void testGlobalConcurrency() throws InterruptedException {
        int nThreads = 16;
        final int iterations = 50000;
        final CountDownLatch latch = new CountDownLatch(nThreads);
        ExecutorService ex = Executors.newFixedThreadPool(nThreads);

        final int maxBlockSize = pageSize / 4;
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        final AtomicBoolean shutdown = new AtomicBoolean(false);

        for (int i = 0; i < nThreads; i++) {
            ex.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Random rand = new Random();
                        MemoryBlock last = null;
                        for (int j = 0; j < iterations; j++) {
                            if (shutdown.get()) {
                                break;
                            }
                            if (error.get() != null) {
                                break;
                            }
                            int size = rand.nextInt(maxBlockSize);
                            if (size == 0) {
                                continue;
                            }
                            try {
                                long address = memoryManager.allocate(size);
                                release(last);
                                last = new MemoryBlock(address, size);
                            } catch (NativeOutOfMemoryError ignored) {
                            } catch (Throwable t) {
                                t.printStackTrace();
                                error.set(t);
                                break;
                            }
                        }
                        release(last);
                    } finally {
                        latch.countDown();
                    }
                }

                private void release(MemoryBlock last) {
                    if (last != null) {
                        memoryManager.free(last.address(), last.size());
                    }
                }
            });
        }

        if (!latch.await(2, TimeUnit.MINUTES)) {
            shutdown.set(true);
            ex.shutdown();
            ex.awaitTermination(30, TimeUnit.SECONDS);
        }

        assertEquals("Memory processor threads have not terminated on time. So there may be possible double free issue !",
                0, latch.getCount());

        Throwable t = error.get();
        assertNull(toString(t), t);
    }

    private static String toString(Throwable t) {
        if (t == null) {
            return "NULL";
        }
        StringWriter s = new StringWriter();
        t.printStackTrace(new PrintWriter(s));
        return s.toString();
    }

    // https://github.com/hazelcast/hazelcast-enterprise/issues/542
    @Test
    public void nativeMemoryStatsShouldBeCheckedAndIncreaseAtomically() throws InterruptedException {
        final long MAX_MEMORY_BLOCK_TO_ALLOCATE = 1024;
        final long MAX_NATIVE_MEMORY_SIZE = MAX_MEMORY_BLOCK_TO_ALLOCATE * 1024;
        final int WORKER_COUNT = 8;

        CountDownLatch latch = new CountDownLatch(WORKER_COUNT);
        NativeMemoryStats nativeMemoryStats = new NativeMemoryStats(MAX_NATIVE_MEMORY_SIZE);
        DummyMalloc malloc = new DummyMalloc(MAX_MEMORY_BLOCK_TO_ALLOCATE);

        try {
            for (int i = 0; i < WORKER_COUNT; i++) {
                spawn(new DummyMemoryAllocatingWorker(malloc, nativeMemoryStats,
                                                      MAX_MEMORY_BLOCK_TO_ALLOCATE, latch));
            }
            // There is no timeout limit because if it doesn't finish in expected time,
            // disposing allocated memory below may cause JVM crash
            // since this address might be used by threads inside the test.
            // Because, in fact all allocations returns same address, all threads play on the same address
            // and all `StandardMemoryManager` instances write on it by resetting it with zero after allocation.
            latch.await();
        } finally {
            malloc.destroy();
        }

        assertTrue("Committed native memory should be " + MAX_NATIVE_MEMORY_SIZE + " at most, " +
                   "but it is " + nativeMemoryStats.getCommittedNativeMemory(),
                   nativeMemoryStats.getCommittedNativeMemory() <= MAX_NATIVE_MEMORY_SIZE);
    }

    private static final class DummyMalloc implements LibMalloc {

        private final long address;

        private DummyMalloc(long size) {
            this.address = UnsafeHelper.UNSAFE.allocateMemory(size);
        }

        @Override
        public long malloc(long size) {
            return address;
        }

        @Override
        public long realloc(long address, long size) {
            return 0L;
        }

        @Override
        public void free(long address) {

        }

        private void destroy() {
            if (address != 0) {
                UnsafeHelper.UNSAFE.freeMemory(address);
            }
        }

    }

    private static final class DummyMemoryAllocatingWorker implements Runnable {

        private final LibMalloc malloc;
        private NativeMemoryStats nativeMemoryStats;
        private final long maxAllocationSize;
        private final CountDownLatch latch;

        private DummyMemoryAllocatingWorker(LibMalloc malloc, NativeMemoryStats nativeMemoryStats,
                                            long maxAllocationSize, CountDownLatch latch) {
            this.malloc = malloc;
            this.nativeMemoryStats = nativeMemoryStats;
            this.maxAllocationSize = maxAllocationSize;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                Random random = new Random();
                StandardMemoryManager mm = new StandardMemoryManager(malloc, nativeMemoryStats);
                for (int i = 0; i < 1000000; i++) {
                    long allocationSize = Math.max(1, random.nextInt((int) maxAllocationSize));
                    try {
                        mm.allocate(allocationSize);
                    } catch (NativeOutOfMemoryError e) {
                        // Ignore, it is expected
                    }
                }
            } finally {
                latch.countDown();
            }
        }

    }

}
