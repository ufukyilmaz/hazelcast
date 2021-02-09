package com.hazelcast.internal.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.memory.GlobalIndexPoolingAllocator.DEFAULT_BTREE_INDEX_NODE_SIZE;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GlobalIndexAllocatorTest extends ParameterizedMemoryTest {

    private GlobalIndexPoolingAllocator memoryAllocator;
    private DelegatingMemoryAllocator delegatingAllocator;
    private LibMalloc malloc;
    private final PooledNativeMemoryStats stats;

    {
        // 16 bytes is for metadata allocation since
        // maxNative is shared from data and metadata space
        long maxNative = MemoryUnit.MEGABYTES.toBytes(1) + 16;
        stats = new PooledNativeMemoryStats(maxNative,
                MemoryUnit.MEGABYTES.toBytes(1), NativeMemoryConfig.DEFAULT_PAGE_SIZE);
    }

    private long nodeSize;

    @Before
    public void setup() {
        checkPlatform();
        malloc = newLibMalloc(allocationSource);
        memoryAllocator = new GlobalIndexPoolingAllocator(malloc, stats, DEFAULT_BTREE_INDEX_NODE_SIZE);
        delegatingAllocator = new DelegatingMemoryAllocator(memoryAllocator);
        nodeSize = memoryAllocator.getNodeSize();
    }

    @After
    public void destroy() {
        if (delegatingAllocator != null) {
            delegatingAllocator.dispose();
        }
        if (malloc != null) {
            malloc.dispose();
        }
    }

    @Test
    public void testAllocate() {
        assertThrows(IllegalArgumentException.class, () -> memoryAllocator.allocate(31));
        assertThrows(IllegalArgumentException.class, () -> memoryAllocator.allocate(0));

        assertEquals(0, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(0, stats.getCommittedNative() - stats.getUsedMetadata());

        long address = delegatingAllocator.allocate(nodeSize);

        // Header is zeroed
        assertEquals(0, AMEM.getLong(address));

        assertEquals(nodeSize, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());

        assertNotEquals(NULL_ADDRESS, address);
        // Access the memory
        AMEM.setMemory(address, nodeSize, (byte) 1);
        assertEquals(0, memoryAllocator.consumeNodeAddressesFromQueue().size());
    }

    @Test
    public void testReallocate() {
        long address = delegatingAllocator.allocate(nodeSize);

        assertThrows(AssertionError.class,
                () -> memoryAllocator.reallocate(address, nodeSize, 2 * nodeSize));

        assertThrows(AssertionError.class,
                () -> memoryAllocator.reallocate(NULL_ADDRESS, nodeSize, nodeSize));

        long newAddress = delegatingAllocator.reallocate(address, nodeSize, nodeSize);

        assertEquals(address, newAddress);
        assertEquals(nodeSize, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());
        delegatingAllocator.free(newAddress, nodeSize);
    }

    @Test
    public void testFree() {
        long address = delegatingAllocator.allocate(nodeSize);
        assertThrows(IllegalArgumentException.class,
                () -> memoryAllocator.free(address, nodeSize + 1));

        assertThrows(AssertionError.class,
                () -> memoryAllocator.free(NULL_ADDRESS, nodeSize));

        // Access the memory
        setMemory(address);
        assertEquals(nodeSize, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());

        delegatingAllocator.free(address, nodeSize);
        assertEquals(1, memoryAllocator.consumeNodeAddressesFromQueue().size());

        // address is still available, and we haven't changed the block
        for (int i = 0; i < nodeSize; ++i) {
            assertEquals(1, AMEM.getByte(address + i));
        }

        assertEquals(0, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());
    }

    @Test
    public void testAllocFreeAlloc() {
        long address = delegatingAllocator.allocate(nodeSize);
        long address2 = delegatingAllocator.allocate(nodeSize);
        delegatingAllocator.free(address, nodeSize);
        delegatingAllocator.free(address2, nodeSize);

        assertEquals(address, delegatingAllocator.allocate(nodeSize));
        assertEquals(address2, delegatingAllocator.allocate(nodeSize));
    }

    @Test
    public void testAllocFree() {
        List<Long> addresses = new ArrayList(10);

        // Allocate 10 blocks
        for (int i = 0; i < 10; ++i) {
            long address = delegatingAllocator.allocate(nodeSize);
            setMemory(address);
            addresses.add(address);
        }

        assertEquals(10 * nodeSize, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(10 * nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());

        assertEquals(0, memoryAllocator.consumeNodeAddressesFromQueue().size());

        // Release the blocks
        addresses.forEach(address -> delegatingAllocator.free(address, nodeSize));

        assertEquals(0, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(10 * nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());

        // Allocate new blocks from the queue
        addresses.forEach(address -> assertEquals((long) address, delegatingAllocator.allocate(nodeSize)));

        assertEquals(0, memoryAllocator.consumeNodeAddressesFromQueue().size());

        // Next malloc will go from the off-heap memory
        long address = delegatingAllocator.allocate(nodeSize);
        assertNotEquals(NULL_ADDRESS, address);
        assertFalse(addresses.contains(address));
        assertEquals(11 * nodeSize, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(11 * nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());
    }

    @Test
    public void testOOM() {
        // Exhaust all memory
        for (int i = 0; i < 128; ++i) {
            delegatingAllocator.allocate(nodeSize);
        }

        assertEquals(0, stats.getFreeNative());

        assertThrows(NativeOutOfMemoryError.class, () -> memoryAllocator.allocate(nodeSize));
    }

    @Test
    public void testDispose() {
        // Allocate 10 blocks
        List<Long> addresses = new ArrayList(10);
        for (int i = 0; i < 10; ++i) {
            long address = memoryAllocator.allocate(nodeSize);
            addresses.add(address);
        }

        assertEquals(10 * nodeSize, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(10 * nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());

        // Remove the blocks
        addresses.forEach(address -> memoryAllocator.free(address, nodeSize));

        assertEquals(0, stats.getUsedNative() - stats.getUsedMetadata());
        assertEquals(10 * nodeSize, stats.getCommittedNative() - stats.getUsedMetadata());

        memoryAllocator.dispose();

        assertEquals(0, stats.getCommittedNative() - stats.getUsedMetadata());

        // Second dispose is no-op
        memoryAllocator.dispose();

        assertEquals(0, stats.getCommittedNative() - stats.getUsedMetadata());
    }

    @Test
    public void testAllocateConcurrency() {
        PooledNativeMemoryStats newStats = new PooledNativeMemoryStats(MemoryUnit.MEGABYTES.toBytes(200),
                MemoryUnit.MEGABYTES.toBytes(10), NativeMemoryConfig.DEFAULT_PAGE_SIZE);

        int newNodeSize = 32;
        GlobalIndexPoolingAllocator newMemoryAllocator = new GlobalIndexPoolingAllocator(malloc, newStats, newNodeSize);

        int threadsCount = 4;

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        int allocationPerThread = 10000;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(threadsCount);
        List<List<Long>> addressesPerThread = new ArrayList<>();
        for (int i = 0; i < threadsCount; ++i) {
            addressesPerThread.add(new ArrayList<>(allocationPerThread));
        }

        for (int i = 0; i < threadsCount; ++i) {
            int index = i;
            executor.submit(() -> {
                try {
                    List<Long> addresses = addressesPerThread.get(index);

                    for (int j = 0; j < allocationPerThread; ++j) {
                        long address = newMemoryAllocator.allocate(newNodeSize);
                        addresses.add(address);
                    }
                    assertAddresses(addresses, newNodeSize);
                } catch (Throwable t) {
                    t.printStackTrace(System.err);
                    exception.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);
        assertNull(exception.get());
        // Check for global duplicates
        Set<Long> allAddresses = new HashSet<>();
        addressesPerThread.forEach(addresses -> allAddresses.addAll(addresses));
        assertEquals(allocationPerThread * threadsCount, allAddresses.size());
        assertEquals(allocationPerThread * threadsCount * newNodeSize, newStats.getUsedNative() - newStats.getUsedMetadata());
        assertEquals(allocationPerThread * threadsCount * newNodeSize, newStats.getCommittedNative() - stats.getUsedMetadata());

        // Release all addresses
        allAddresses.forEach(address -> newMemoryAllocator.free(address, newNodeSize));
        assertEquals(0, newStats.getUsedNative() - newStats.getUsedMetadata());
        assertEquals(allocationPerThread * threadsCount * newNodeSize, newStats.getCommittedNative() - newStats.getUsedMetadata());

        executor.shutdownNow();
        newMemoryAllocator.dispose();
    }

    @Test
    public void testAllocateFreeConcurrency() {
        PooledNativeMemoryStats newStats = new PooledNativeMemoryStats(MemoryUnit.MEGABYTES.toBytes(200),
                MemoryUnit.MEGABYTES.toBytes(10), NativeMemoryConfig.DEFAULT_PAGE_SIZE);

        int newNodeSize = 32;
        GlobalIndexPoolingAllocator newMemoryAllocator = new GlobalIndexPoolingAllocator(malloc, newStats, newNodeSize);

        int threadsCount = 4;

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        int allocationPerThread = 10000;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; ++i) {
            executor.submit(() -> {
                try {
                    List<Long> addresses = new ArrayList<>(allocationPerThread / 4);

                    for (int n = 0; n < 4; ++n) {
                        for (int j = 0; j < allocationPerThread / 4; ++j) {
                            long address = newMemoryAllocator.allocate(newNodeSize);
                            addresses.add(address);
                        }
                        assertAddresses(addresses, newNodeSize);

                        // Release the blocs
                        addresses.forEach(address -> newMemoryAllocator.free(address, newNodeSize));
                        addresses.clear();
                    }
                } catch (Throwable t) {
                    t.printStackTrace(System.err);
                    exception.compareAndSet(null, t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertOpenEventually(latch);
        assertNull(exception.get());
        assertEquals(0, newStats.getUsedNative() - newStats.getUsedMetadata());
        executor.shutdownNow();
        newMemoryAllocator.dispose();
    }

    @Test
    public void testSetNodeSize() {
        assertEquals(DEFAULT_BTREE_INDEX_NODE_SIZE, nodeSize);
        assertThrows(IllegalArgumentException.class, () -> new GlobalIndexPoolingAllocator(malloc, stats, 127));

        GlobalIndexPoolingAllocator newMemoryAllocator = new GlobalIndexPoolingAllocator(malloc, stats, 256);
        assertEquals(256, newMemoryAllocator.getNodeSize());
    }

    private void setMemory(long address) {
        AMEM.setMemory(address, nodeSize, (byte) 1);
    }

    private void assertAddresses(Collection<Long> addresses, long nodeSize) {
        // Check no NULLs
        addresses.forEach(address -> assertNotEquals(NULL_ADDRESS, (long) address));
        // Check for duplicates
        TreeSet<Long> addressesSet = new TreeSet<>(addresses);
        assertEquals(addresses.size(), addressesSet.size());

        // Check blocks are not intersecting
        for (long blockAddr : addresses) {
            Long higher = addressesSet.higher(blockAddr);
            if (higher != null) {
                assertTrue("blocAddr " + blockAddr + ", higher " + higher, blockAddr + nodeSize <= higher);
            }
        }
    }

    private class DelegatingMemoryAllocator implements MemoryAllocator {

        private final MemoryAllocator delegate;
        private final Set<Long> allocatedAddresses;

        DelegatingMemoryAllocator(MemoryAllocator delegate) {
            this.delegate = delegate;
            this.allocatedAddresses = ConcurrentHashMap.newKeySet();
        }

        @Override
        public long allocate(long size) {
            long address = delegate.allocate(size);
            allocatedAddresses.add(address);
            return address;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            long newAddress = delegate.reallocate(address, currentSize, newSize);
            allocatedAddresses.remove(address);
            allocatedAddresses.add(newAddress);
            return newAddress;
        }

        @Override
        public void free(long address, long size) {
            delegate.free(address, size);
            allocatedAddresses.remove(address);
        }

        @Override
        public void dispose() {
            delegate.dispose();
            allocatedAddresses.forEach(address -> malloc.free(address));
        }
    }
}
