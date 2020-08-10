package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.ParameterizedMemoryTest.AllocationSource;
import com.hazelcast.internal.memory.impl.HeapMemoryManager;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.ParameterizedMemoryTest.AllocationSource.MEMKIND_DRAM;
import static com.hazelcast.internal.memory.ParameterizedMemoryTest.AllocationSource.MEMKIND_PMEM;
import static com.hazelcast.internal.memory.ParameterizedMemoryTest.AllocationSource.UNSAFE;
import static com.hazelcast.internal.memory.ParameterizedMemoryTest.newLibMallocFactory;
import static com.hazelcast.internal.memory.impl.MemkindHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static com.hazelcast.internal.memory.impl.MemkindUtil.HD_MEMKIND;
import static com.hazelcast.internal.util.QuickMath.modPowerOfTwo;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.test.HazelcastTestSupport.assumeThatLinuxOS;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class MemoryAllocatorTest {

    public enum ManagerType {
        HEAP, STANDARD, SYSTEM, POOLED_GLOBAL, POOLED_THREADLOCAL
    }

    @Parameters(name = "managerType:{0}, allocationSource: {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {ManagerType.HEAP, UNSAFE},
                {ManagerType.HEAP, MEMKIND_PMEM},
                {ManagerType.HEAP, MEMKIND_DRAM},
                {ManagerType.STANDARD, UNSAFE},
                {ManagerType.STANDARD, MEMKIND_PMEM},
                {ManagerType.STANDARD, MEMKIND_DRAM},
                {ManagerType.SYSTEM, UNSAFE},
                {ManagerType.SYSTEM, MEMKIND_PMEM},
                {ManagerType.SYSTEM, MEMKIND_DRAM},
                {ManagerType.POOLED_GLOBAL, UNSAFE},
                {ManagerType.POOLED_GLOBAL, MEMKIND_PMEM},
                {ManagerType.POOLED_GLOBAL, MEMKIND_DRAM},
                {ManagerType.POOLED_THREADLOCAL, UNSAFE},
                {ManagerType.POOLED_THREADLOCAL, MEMKIND_PMEM},
                {ManagerType.POOLED_THREADLOCAL, MEMKIND_DRAM},
                });
    }

    @Parameter(0)
    public ManagerType mgrType;

    @Parameter(1)
    public AllocationSource allocationSource;

    private MemoryManager memMgr;
    private Disposable toDispose;

    @BeforeClass
    public static void init() {
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
        System.clearProperty(HD_MEMKIND);
    }

    @Before
    public void setup() {
        if (allocationSource != UNSAFE) {
            assumeThatLinuxOS();
        }
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
        return new PoolingMemoryManager(new MemorySize(32, MEGABYTES), 16, 1 << 20,
                newLibMallocFactory(allocationSource));
    }

    @After
    public void teardown() {
        if (toDispose != null) {
            toDispose.dispose();
        }
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
