package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import sun.misc.Unsafe;

/**
 * @author mdogan 02/06/14
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemoryAllocatorTest {

    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    @Test
    public void testMallocFreeStandard() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        testMallocFree(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testMallocFreeGlobalPooled() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testMallocFree(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testMallocFreeThreadLocalPooled() {
        PoolingMemoryManager memoryManager = new PoolingMemoryManager(
                new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        memoryManager.registerThread(Thread.currentThread());
        testMallocFree(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testMallocPooledSystem() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testMallocFree(memoryManager.unwrapMemoryAllocator());
        memoryManager.destroy();
    }

    private void testMallocFree(MemoryAllocator memoryAllocator) {
        long address1 = memoryAllocator.allocate(5);
        UNSAFE.putInt(address1, -123);
        memoryAllocator.free(address1, 5);

        long address2 = memoryAllocator.allocate(11);
        UNSAFE.putLong(address2, -1234567L);
        memoryAllocator.free(address2, 11);
    }
}
