package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.internal.memory.HazelcastMemoryManager.SIZE_INVALID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastMemoryManagerTest extends ParameterizedMemoryTest {

    private static final int DEFAULT_PAGE_SIZE = 1 << 10;

    private HazelcastMemoryManager memoryManager;
    private LibMalloc malloc;
    private final PooledNativeMemoryStats stats = new PooledNativeMemoryStats(MemoryUnit.MEGABYTES.toBytes(1),
            MemoryUnit.MEGABYTES.toBytes(1));

    @Before
    public void setup() {
        checkPlatform();
        malloc = newLibMalloc(persistentMemory);
    }

    @After
    public void destroy() {
        if (memoryManager != null) {
            memoryManager.dispose();
        }
        if (malloc != null) {
            malloc.dispose();
        }
    }

    @Test
    public void testValidateAndGetSize_standard() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        long address = memoryManager.allocate(32);

        assertEquals(SIZE_INVALID, memoryManager.getUsableSize(0L));
        assertEquals(SIZE_INVALID, memoryManager.getAllocatedSize(0L));
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetUsableSize(0L));
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetAllocatedSize(0L));
        assertEquals(SIZE_INVALID, memoryManager.getUsableSize(address));
        assertEquals(SIZE_INVALID, memoryManager.getAllocatedSize(address));
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetUsableSize(address));
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetAllocatedSize(address));
    }

    @Test
    public void testValidateAndGetUsableSize_threadLocalPooled() throws Exception {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetUsableSize(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_threadLocalPooled_withUnallocatedAddress() throws Exception {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetUsableSize_withUnallocatedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_threadLocalPooled_withFreedAddress() throws Exception {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetUsableSize_withFreedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_threadLocalPooled() throws Exception {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetAllocatedSize(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_threadLocalPooled_withUnallocatedAddress() throws Exception {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetAllocatedSize_withUnallocatedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_threadLocalPooled_withFreedAddress() throws Exception {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetAllocatedSize_withFreedAddress(memoryManager);
    }

    private ThreadLocalPoolingMemoryManager newThreadLocalPoolingMemoryManager() {
        return new ThreadLocalPoolingMemoryManager(16, DEFAULT_PAGE_SIZE, malloc, stats);
    }

    @Test
    public void testValidateAndGetUsableSize_globalPooled() throws Exception {
        memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetUsableSize(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_globalPooled_withUnallocatedAddress() throws Exception {
        memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetUsableSize_withUnallocatedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_globalPooled_withFreedAddress() throws Exception {
        memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetUsableSize_withFreedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_globalPooled() throws Exception {
        memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetAllocatedSize(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_globalPooled_withUnallocatedAddress() throws Exception {
        memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetAllocatedSize_withUnallocatedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_globalPooled_withFreedAddress() throws Exception {
        memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetAllocatedSize_withFreedAddress(memoryManager);
    }

    private GlobalPoolingMemoryManager newGlobalPoolingMemoryManager() {
        return new GlobalPoolingMemoryManager(16, DEFAULT_PAGE_SIZE, malloc, stats, new SimpleGarbageCollector());
    }

    @Test
    public void testValidateAndGetUsableSize_pooling() throws Exception {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testValidateAndGetUsableSize(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_pooling_withUnallocatedAddress() throws Exception {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testValidateAndGetUsableSize_withUnallocatedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_pooling_withFreedAddress() throws Exception {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testValidateAndGetUsableSize_withFreedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_pooling() throws Exception {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testValidateAndGetAllocatedSize(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_pooling_withUnallocatedAddress() throws Exception {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testValidateAndGetAllocatedSize_withUnallocatedAddress(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_pooling_withFreedAddress() throws Exception {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testValidateAndGetAllocatedSize_withFreedAddress(memoryManager);
    }

    private void testValidateAndGetUsableSize(HazelcastMemoryManager memoryManager) throws Exception {
        int size = 31;
        long address = memoryManager.allocate(size);

        long validatedSize = memoryManager.validateAndGetUsableSize(address);
        assertTrue("Size= " + size + ", Validated-size= " + validatedSize, size <= validatedSize);
    }

    private void testValidateAndGetUsableSize_withUnallocatedAddress(HazelcastMemoryManager memoryManager) throws Exception {
        int size = 32;
        long address = memoryManager.allocate(size);
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetUsableSize(address + size));
    }

    private void testValidateAndGetUsableSize_withFreedAddress(HazelcastMemoryManager memoryManager) throws Exception {
        int size = 31;
        long address = memoryManager.allocate(size);
        memoryManager.free(address, size);
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetUsableSize(address));
    }

    private void testValidateAndGetAllocatedSize(HazelcastMemoryManager memoryManager) throws Exception {
        int size = 31;
        long address = memoryManager.allocate(size);

        long validatedSize = memoryManager.validateAndGetAllocatedSize(address);
        assertTrue("Size= " + size + ", Validated-size= " + validatedSize, size < validatedSize);
    }

    private void testValidateAndGetAllocatedSize_withUnallocatedAddress(HazelcastMemoryManager memoryManager) throws Exception {
        int size = 32;
        long address = memoryManager.allocate(size);
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetAllocatedSize(address + size));
    }

    private void testValidateAndGetAllocatedSize_withFreedAddress(HazelcastMemoryManager memoryManager) throws Exception {
        int size = 31;
        long address = memoryManager.allocate(size);
        memoryManager.free(address, size);
        assertEquals(SIZE_INVALID, memoryManager.validateAndGetAllocatedSize(address));
    }

    @Test
    public void testNewSequence_standard() {
        memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        testNewSequence(memoryManager);
    }

    @Test
    public void testNewSequence_threadLocalPooled() {
        memoryManager = newThreadLocalPoolingMemoryManager();
        testNewSequence(memoryManager);
    }

    @Test
    public void testNewSequence_globalPooled() {
        memoryManager = newGlobalPoolingMemoryManager();
        testNewSequence(memoryManager);
    }

    @Test
    public void testNewSequence_pooling() {
        memoryManager = new PoolingMemoryManager(new MemorySize(8, MemoryUnit.MEGABYTES));
        testNewSequence(memoryManager);
    }

    private void testNewSequence(HazelcastMemoryManager memoryManager) {
        long initialSeq = memoryManager.newSequence();
        for (int k = 0; k < 1000; k++) {
            assertEquals(++initialSeq, memoryManager.newSequence());
        }
    }

    @Test
    public void testValidateAndGetUsableSize_threadLocalPooled_withBiggerThanPageSize() {
        HazelcastMemoryManager memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetUsableSize_withBiggerThanPageSize(memoryManager);
    }

    @Test
    public void testValidateAndGetUsableSize_globalPooled_withBiggerThanPageSize() {
        HazelcastMemoryManager memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetUsableSize_withBiggerThanPageSize(memoryManager);
    }

    private void testValidateAndGetUsableSize_withBiggerThanPageSize(HazelcastMemoryManager memoryManager) {
        final int allocationSize = 2 * DEFAULT_PAGE_SIZE + 3;

        long address = memoryManager.allocate(allocationSize);

        long validatedSize = memoryManager.validateAndGetUsableSize(address);
        assertEquals("Size= " + allocationSize + ", Validated-size= " + validatedSize, allocationSize, validatedSize);

        memoryManager.free(address, allocationSize);

        memoryManager.dispose();
    }

    @Test
    public void testValidateAndGetAllocatedSize_threadLocalPooled_withBiggerThanPageSize() {
        HazelcastMemoryManager memoryManager = newThreadLocalPoolingMemoryManager();
        testValidateAndGetAllocatedSize_withBiggerThanPageSize(memoryManager);
    }

    @Test
    public void testValidateAndGetAllocatedSize_globalPooled_withBiggerThanPageSize() {
        HazelcastMemoryManager memoryManager = newGlobalPoolingMemoryManager();
        testValidateAndGetAllocatedSize_withBiggerThanPageSize(memoryManager);
    }

    private void testValidateAndGetAllocatedSize_withBiggerThanPageSize(HazelcastMemoryManager memoryManager) {
        final int allocationSize = 2 * DEFAULT_PAGE_SIZE + 3;

        long address = memoryManager.allocate(allocationSize);

        long validatedSize = memoryManager.validateAndGetAllocatedSize(address);
        assertTrue("Size= " + allocationSize + ", Validated-size= " + validatedSize, allocationSize <= validatedSize);

        memoryManager.free(address, allocationSize);

        memoryManager.dispose();
    }

}
