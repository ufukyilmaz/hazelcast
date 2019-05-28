package com.hazelcast.memory;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.HeapMemoryManager;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.nio.Disposable;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemoryAllocatorTest {

    public enum ManagerType {
        HEAP, STANDARD, SYSTEM, POOLED_GLOBAL, POOLED_THREADLOCAL
    }

    @Parameterized.Parameters(name = "managerType:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {ManagerType.HEAP},
                {ManagerType.STANDARD},
                {ManagerType.SYSTEM},
                {ManagerType.POOLED_GLOBAL},
                {ManagerType.POOLED_THREADLOCAL},
        });
    }

    @Parameterized.Parameter
    public ManagerType mgrType;

    private MemoryManager memMgr;
    private Disposable toDispose;

    @Before
    public void setup() {
        switch (mgrType) {
            case HEAP:
                memMgr = new HeapMemoryManager(8 << 20);
                toDispose = memMgr;
                break;
            case STANDARD:
                memMgr = new MemoryManagerBean(
                        new StandardMemoryManager(new MemorySize(1, MEGABYTES)), MEM);
                toDispose = memMgr;
                break;
            case SYSTEM:
                PoolingMemoryManager malloc = getMalloc();
                memMgr = new MemoryManagerBean(malloc.getSystemAllocator(), MEM);
                toDispose = malloc;
                break;
            case POOLED_GLOBAL:
                memMgr = new MemoryManagerBean(getMalloc(), MEM);
                toDispose = memMgr;
                break;
            case POOLED_THREADLOCAL:
                PoolingMemoryManager pooledThreadLocalMalloc = getMalloc();
                pooledThreadLocalMalloc.registerThread(Thread.currentThread());
                memMgr = new MemoryManagerBean(pooledThreadLocalMalloc, MEM);
                toDispose = memMgr;
                break;
        }
    }

    private PoolingMemoryManager getMalloc() {
        return new PoolingMemoryManager(new MemorySize(8, MEGABYTES), 16, 1 << 20);
    }

    @After
    public void teardown() {
        toDispose.dispose();
    }

    @Test
    public void testMallocFree() {
        final MemoryAllocator malloc = memMgr.getAllocator();
        final MemoryAccessor mem = memMgr.getAccessor();
        long address1 = malloc.allocate(5);
        checkZero(mem, address1, 5);
        mem.putInt(address1, -123);
        malloc.free(address1, 5);
        long address2 = malloc.allocate(11);
        checkZero(mem, address2, 11);
        mem.putLong(address2, -1234567L);
        malloc.free(address2, 11);
    }

    @Test
    public void testReallocExpand() {
        final MemoryAllocator malloc = memMgr.getAllocator();
        final MemoryAccessor mem = memMgr.getAccessor();
        long address = malloc.allocate(8);
        long value = -123;
        mem.putLong(address, value);
        address = malloc.reallocate(address, 8, 16);
        assertEquals(value, mem.getLong(address));
        checkZero(mem, address + 8, 8);
    }

    @Test
    public void testReallocShrink() {
        final MemoryAllocator malloc = memMgr.getAllocator();
        final MemoryAccessor mem = memMgr.getAccessor();
        long address1 = malloc.allocate(8);
        int value = -123;
        mem.putInt(address1, value);
        address1 = malloc.reallocate(address1, 8, 4);
        assertEquals(value, mem.getInt(address1));
    }

    @Test
    public void testMalloc_8bytes_Aligned() {
        if (memMgr.getAccessor() != MEM) {
            return;
        }
        testMalloc_8bytes_Aligned(5);
        testMalloc_8bytes_Aligned(55);
        testMalloc_8bytes_Aligned(555);
        testMalloc_8bytes_Aligned(5555);
        testMalloc_8bytes_Aligned(55555);
    }

    private void testMalloc_8bytes_Aligned(int size) {
        final MemoryAllocator malloc = memMgr.getAllocator();
        long address = malloc.allocate(size);
        assertEquals("Address: " + address + " is not aligned!", 0, modPowerOfTwo(address, 8));
        malloc.free(address, size);
    }

    private static void checkZero(MemoryAccessor mem, long address, int len) {
        for (int i = 0; i < len; i++) {
            byte b = mem.getByte(address + i);
            assertEquals(0, b);
        }
    }
}
