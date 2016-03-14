package com.hazelcast.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.UnsafeMalloc;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NativeMemoryStatsTest {

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
                        "but it is " + nativeMemoryStats.getCommittedNative(),
                nativeMemoryStats.getCommittedNative() <= MAX_NATIVE_MEMORY_SIZE);
    }

    private static final class DummyMalloc implements LibMalloc {

        private final LibMalloc libMalloc;
        private final long address;

        private DummyMalloc(long size) {
            this.libMalloc = new UnsafeMalloc();
            this.address = libMalloc.malloc(size);
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
                libMalloc.free(address);
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
