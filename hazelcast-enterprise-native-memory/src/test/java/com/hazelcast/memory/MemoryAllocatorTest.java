package com.hazelcast.memory;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;
import static org.junit.Assert.assertEquals;

import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 02/06/14
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemoryAllocatorTest {

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
    public void testMallocFreePooledSystem() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testMallocFree(memoryManager.unwrapMemoryAllocator());
        memoryManager.destroy();
    }

    private static void testMallocFree(MemoryAllocator memoryAllocator) {
        long address1 = memoryAllocator.allocate(5);
        checkZero(address1, 5);
        MEM.putInt(address1, -123);
        memoryAllocator.free(address1, 5);

        long address2 = memoryAllocator.allocate(11);
        checkZero(address2, 11);
        MEM.putLong(address2, -1234567L);
        memoryAllocator.free(address2, 11);
    }

    private static void checkZero(long address, int len) {
        for (int i = 0; i < len; i++) {
            byte b = MEM.getByte(address + i);
            assertEquals(0, b);
        }
    }

    @Test
    public void testReallocExpandStandard() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        testReallocExpand(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testReallocExpandGlobalPooled() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testReallocExpand(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testReallocExpandThreadLocalPooled() {
        PoolingMemoryManager memoryManager = new PoolingMemoryManager(
                new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        memoryManager.registerThread(Thread.currentThread());
        testReallocExpand(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testReallocExpandPooledSystem() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testReallocExpand(memoryManager.unwrapMemoryAllocator());
        memoryManager.destroy();
    }

    private static void testReallocExpand(MemoryAllocator memoryAllocator) {
        long address = memoryAllocator.allocate(8);
        long value = -123;
        MEM.putLong(address, value);
        address = memoryAllocator.reallocate(address, 8, 16);
        assertEquals(value, MEM.getLong(address));
        checkZero(address + 8, 8);
    }

    @Test
    public void testReallocShrinkStandard() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        testReallocShrink(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testReallocShrinkGlobalPooled() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testReallocShrink(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testReallocShrinkThreadLocalPooled() {
        PoolingMemoryManager memoryManager = new PoolingMemoryManager(
                new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        memoryManager.registerThread(Thread.currentThread());
        testReallocShrink(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testReallocShrinkPooledSystem() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testReallocShrink(memoryManager.unwrapMemoryAllocator());
        memoryManager.destroy();
    }

    private static void testReallocShrink(MemoryAllocator memoryAllocator) {
        long address1 = memoryAllocator.allocate(8);
        int value = -123;
        MEM.putInt(address1, value);
        address1 = memoryAllocator.reallocate(address1, 8, 4);
        assertEquals(value, MEM.getInt(address1));
    }

    @Test
    public void testMalloc_8bytes_Aligned_Standard() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        testMalloc_8bytes_Aligned(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testMalloc_8bytes_Aligned_GlobalPooled() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testMalloc_8bytes_Aligned(memoryManager);
        memoryManager.destroy();
    }

    @Test
    public void testMalloc_8bytes_Aligned_ThreadLocalPooled() {
        PoolingMemoryManager memoryManager = new PoolingMemoryManager(
                new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        memoryManager.registerThread(Thread.currentThread());
        testMalloc_8bytes_Aligned(memoryManager);
        memoryManager.destroy();
    }

    private static void testMalloc_8bytes_Aligned(MemoryAllocator memoryAllocator) {
        testMalloc_8bytes_Aligned(memoryAllocator, 5);
        testMalloc_8bytes_Aligned(memoryAllocator, 55);
        testMalloc_8bytes_Aligned(memoryAllocator, 555);
        testMalloc_8bytes_Aligned(memoryAllocator, 5555);
        testMalloc_8bytes_Aligned(memoryAllocator, 55555);
    }

    private static void testMalloc_8bytes_Aligned(MemoryAllocator memoryAllocator, int size) {
        long address = memoryAllocator.allocate(size);
        assertTrue("Address: " + address + " is not aligned!", modPowerOfTwo(address, 8) == 0);
        memoryAllocator.free(address, size);
    }

}
