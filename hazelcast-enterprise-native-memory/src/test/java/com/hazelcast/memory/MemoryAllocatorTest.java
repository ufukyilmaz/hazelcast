package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import sun.misc.Unsafe;

import static org.junit.Assert.assertEquals;

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
    public void testMallocFreePooledSystem() {
        MemoryManager memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES), 16, 1 << 20);
        testMallocFree(memoryManager.unwrapMemoryAllocator());
        memoryManager.destroy();
    }

    private void testMallocFree(MemoryAllocator memoryAllocator) {
        long address1 = memoryAllocator.allocate(5);
        checkZero(address1, 5);
        UNSAFE.putInt(address1, -123);
        memoryAllocator.free(address1, 5);

        long address2 = memoryAllocator.allocate(11);
        checkZero(address2, 11);
        UNSAFE.putLong(address2, -1234567L);
        memoryAllocator.free(address2, 11);
    }

    private void checkZero(long address, int len) {
        for (int i = 0; i < len; i++) {
            byte b = UnsafeHelper.UNSAFE.getByte(address + i);
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

    private void testReallocExpand(MemoryAllocator memoryAllocator) {
        long address = memoryAllocator.allocate(8);
        long value = -123;
        UNSAFE.putLong(address, value);
        address = memoryAllocator.reallocate(address, 8, 16);
        assertEquals(value, UNSAFE.getLong(address));
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

    private void testReallocShrink(MemoryAllocator memoryAllocator) {
        long address1 = memoryAllocator.allocate(8);
        int value = -123;
        UNSAFE.putInt(address1, value);
        address1 = memoryAllocator.reallocate(address1, 8, 4);
        assertEquals(value, UNSAFE.getInt(address1));
    }
}
