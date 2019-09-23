package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_PAGE_SIZE;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class GlobalPoolingMemoryManagerStressTest extends ParameterizedMemoryTest {

    private GlobalPoolingMemoryManager globalPoolingMemoryManager;
    private LibMalloc malloc;
    private PooledNativeMemoryStats stats;
    private SimpleGarbageCollector gc;

    @Before
    public void setUp() throws Exception {
        checkPlatform();
        long maxNative = MEGABYTES.toBytes(32);
        long maxMetadata = MEGABYTES.toBytes(4);

        int minBlockSize = DEFAULT_MIN_BLOCK_SIZE;
        int pageSize = DEFAULT_PAGE_SIZE;
        stats = new PooledNativeMemoryStats(maxNative, maxMetadata);

        gc = new SimpleGarbageCollector();
        gc.start();

        malloc = newLibMalloc(persistentMemory);
        globalPoolingMemoryManager = new GlobalPoolingMemoryManager(minBlockSize, pageSize, malloc, stats, gc);
    }

    @After
    public void tearDown() {
        if (malloc != null) {
            malloc.dispose();
        }
        if (gc != null) {
            gc.abort();
        }
    }

    @Test
    public void testConcurrentAllocations() throws InterruptedException {
        int threadCount = 4;
        long duration = MINUTES.toMillis(2);

        long deadLine = System.currentTimeMillis() + duration;
        AllocatorThread[] threads = startAllocatorThreads(threadCount, deadLine);
        monitorUsedMemoryUntilDeadline(deadLine);

        assertAllocatorsDiedPeacefully(threads);
    }

    private void assertAllocatorsDiedPeacefully(AllocatorThread[] threads) {
        assertJoinable(threads);
        for (AllocatorThread thread : threads) {
            assertNull(thread.throwable);
        }
    }

    private void monitorUsedMemoryUntilDeadline(long deadLine) throws InterruptedException {
        while (System.currentTimeMillis() < deadLine) {
            long usedNative = stats.getUsedNative();
            long nativeUsedMB = MemoryUnit.BYTES.toMegaBytes(usedNative);
            System.out.println("Used native: " + nativeUsedMB + " MB");
            Thread.sleep(1000);
        }
    }

    private AllocatorThread[] startAllocatorThreads(int threadCount, long deadLine) {
        ConcurrentLinkedQueue<AllocatedBlock> allocatedBlocks = new ConcurrentLinkedQueue<AllocatedBlock>();
        AllocatorThread[] threads = new AllocatorThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            AllocatorThread thread = new AllocatorThread(globalPoolingMemoryManager, allocatedBlocks, deadLine);
            threads[i] = thread;
            thread.start();
        }
        return threads;
    }

    private static class AllocatorThread extends Thread {

        private final Queue<AllocatedBlock> allocatedBlockQueue;
        private final GlobalPoolingMemoryManager memoryManager;
        private final long deadline;

        private volatile Throwable throwable;

        private AllocatorThread(GlobalPoolingMemoryManager memoryManager, Queue blockQueue, long deadline) {
            this.allocatedBlockQueue = blockQueue;
            this.memoryManager = memoryManager;
            this.deadline = deadline;
        }


        @Override
        public void run() {
            Random r = new Random();
            try {
                while (System.currentTimeMillis() < deadline) {
                    doStep(r);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                throwable = t;
            }
        }

        private void doStep(Random r) {
            int blockCount = r.nextInt(100) + 5;
            for (int i = 0; i < blockCount; i++) {
                int size = r.nextInt(300) + 1000;
                long address = memoryManager.allocate(size);
                allocatedBlockQueue.add(new AllocatedBlock(address, size));
            }

            blockCount = r.nextInt(105) + 5;
            for (int i = 0; i < blockCount; i++) {
                AllocatedBlock allocatedBlock = allocatedBlockQueue.poll();
                if (allocatedBlock == null) {
                    break;
                }
                memoryManager.free(allocatedBlock.address, allocatedBlock.size);
            }
        }
    }

    private static class AllocatedBlock {

        private long address;
        private long size;

        private AllocatedBlock(long address, long size) {
            this.address = address;
            this.size = size;
        }
    }
}
