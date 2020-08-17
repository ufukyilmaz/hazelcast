package com.hazelcast.internal.bplustree;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeMemoryTest extends BPlusTreeTestSupport {

    private Set<Long> allocatedDefaultAddressesOwnedByBPlusTree = new HashSet<>();
    private Set<Long> allocatedIndexAddresses = new HashSet<>();
    private int throwIndexMemoryManagerOOMCounter = Integer.MAX_VALUE;
    private int throwKeyMemoryManagerOOMCounter = Integer.MAX_VALUE;
    private boolean freedMemory;

    @SuppressWarnings("checkstyle:ParameterNumber")
    @Override
    HDBPlusTree newBPlusTree(EnterpriseSerializationService ess,
                             MemoryAllocator keyAllocator,
                             MemoryAllocator indexAllocator, LockManager lockManager,
                             BPlusTreeKeyComparator keyComparator,
                             BPlusTreeKeyAccessor keyAccessor,
                             MapEntryFactory entryFactory,
                             int nodeSize,
                             int indexScanBatchSize,
                             EntrySlotPayload entrySlotPayload) {
        return HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator, lockManager, keyComparator, keyAccessor,
                entryFactory, nodeSize, 0, entrySlotPayload);
    }

    @Override
    HazelcastMemoryManager newKeyAllocator(PoolingMemoryManager poolingMemoryManager) {
        HazelcastMemoryManager memoryAllocator = poolingMemoryManager.getGlobalMemoryManager();
        return new DelegatingDefaultMemoryAllocator(memoryAllocator);
    }

    DelegatingMemoryAllocator newDelegatingIndexMemoryAllocator(MemoryAllocator indexAllocator, AllocatorCallback callback) {
        return new DelegatingMemoryAllocator(indexAllocator, new IndexMemoryAllocatorCallback());
    }

    @Override
    NativeMemoryData toNativeData(Object value) {
        NativeMemoryData data = super.toNativeData(value);
        allocatedDefaultAddressesOwnedByBPlusTree.remove(data.address());
        return data;
    }

    @Test
    public void testClearDisposesMemory() {
        insertKeysCompact(1000);
        assertEquals(3, getNodeLevel(rootAddr));
        btree.clear();
        assertEquals(0, getKeysCount(rootAddr));
        assertEquals(1, getNodeLevel(rootAddr));
        long childAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        assertNotEquals(NULL_ADDRESS, childAddr);
        assertEquals(0, getNodeLevel(childAddr));
        assertEquals(0, getKeysCount(childAddr));

        assertEquals(0, allocatedDefaultAddressesOwnedByBPlusTree.size());
        assertEquals(2, allocatedIndexAddresses.size());
        assertTrue(allocatedIndexAddresses.contains(rootAddr));
        assertTrue(allocatedIndexAddresses.contains(innerNodeAccessor.getValueAddr(rootAddr, 0)));
    }

    @Test
    public void testNoMemoryLeakOnOOM() {
        insertKeysCompact(81);
        assertEquals(1, getNodeLevel(rootAddr));
        assertTrue(innerNodeAccessor.isNodeFull(rootAddr));
        // One more key will cause root split, skip OOM on incrementing depth
        throwIndexMemoryManagerOOMCounter = 2;
        allocatedDefaultAddressesOwnedByBPlusTree.clear();
        allocatedIndexAddresses.clear();
        try {
            insertKey(81);
            fail("NativeOutOfMemoryError is expected");
        } catch (NativeOutOfMemoryError oom) {
            assertEquals(0, allocatedDefaultAddressesOwnedByBPlusTree.size());
            // Depth has been increased, but split failed
            assertEquals(1, allocatedIndexAddresses.size());
        }
        assertEquals(81, queryKeysCount());
    }

    @Test
    public void testNoMemoryLeakOnLeafSplitOOM() {
        insertKeysCompact(9);
        assertEquals(1, getNodeLevel(rootAddr));
        // One more key will cause leaf split
        throwIndexMemoryManagerOOMCounter = 1;
        allocatedDefaultAddressesOwnedByBPlusTree.clear();
        allocatedIndexAddresses.clear();
        try {
            insertKey(9);
            fail("NativeOutOfMemoryError is expected");
        } catch (NativeOutOfMemoryError oom) {
            assertEquals(0, allocatedDefaultAddressesOwnedByBPlusTree.size());
            assertEquals(0, allocatedIndexAddresses.size());
        }
        assertEquals(9, queryKeysCount());
        assertEquals(1, getNodeLevel(rootAddr));
    }

    @Test
    public void testNoMemoryLeakOnDispose() {
        insertKeysCompact(1000);
        assertEquals(3, getNodeLevel(rootAddr));
        btree.dispose();
        assertTrue(allocatedDefaultAddressesOwnedByBPlusTree.isEmpty());
        assertTrue(allocatedIndexAddresses.isEmpty());
    }

    @Test
    public void testDoubleDispose() {
        insertKeysCompact(1000);
        assertEquals(3, getNodeLevel(rootAddr));
        btree.dispose();
        assertTrue(btree.isDisposed());
        assertTrue(freedMemory);
        freedMemory = false;
        btree.dispose();
        assertFalse(freedMemory);
        assertTrue(btree.isDisposed());
    }

    @Test
    public void testDisposeWhileIterating() {
        insertKeysCompact(1000);
        assertEquals(3, getNodeLevel(rootAddr));

        Iterator it = btree.lookup(null, true, null, true);
        // Consume some elements
        for (int i = 0; i < 300; ++i) {
            assertTrue(it.hasNext());
            it.next();
        }

        btree.dispose();
        assertThrows(HazelcastException.class, () -> it.hasNext());
    }

    @Test
    public void testDisposeWhileConcurrentlyAccessing() {
        final int keysCount = 10000;
        insertKeysCompact(keysCount);

        int threadsCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        final AtomicReference<Throwable>[] exceptions = new AtomicReference[threadsCount];
        for (int i = 0; i < threadsCount; ++i) {
            exceptions[i] = new AtomicReference<>();
        }

        CountDownLatch latch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; ++i) {
            final int threadIndex = i;

            executor.submit(() -> {
                try {
                    for (; ; ) {
                        boolean update = nextBoolean();
                        if (update) {
                            boolean insert = nextBoolean();
                            int index = nextInt(keysCount);
                            if (insert) {
                                insertKey(index);
                            } else {
                                btree.remove(index, nativeData("Name_" + index));
                                insertKey(index);
                            }
                        } else {
                            queryKeysCount();
                        }
                    }
                } catch (Throwable t) {
                    exceptions[threadIndex].set(t);
                } finally {
                    latch.countDown();
                }
            });
        }

        sleepSeconds(3);
        btree.dispose();
        assertTrue(btree.isDisposed());

        assertOpenEventually(latch);
        for (int i = 0; i < threadsCount; ++i) {
            Throwable e = exceptions[i].get();
            assertNotNull(e);
            assertTrue(e instanceof HazelcastException);
            assertTrue(e.getMessage().contains("Disposed B+tree cannot be accessed"));
        }

        executor.shutdownNow();
    }

    @Test
    public void testDisposeMemoryOnRemoveFromLeaf() {
        insertKey(1);
        assertEquals(1, allocatedDefaultAddressesOwnedByBPlusTree.size());
        removeKey(1);
        assertEquals(0, allocatedDefaultAddressesOwnedByBPlusTree.size());
    }

    @Test
    public void testDisposeMemoryOnRemoveFromInner() {
        insertKeys(10);
        for (int i = 0; i < 10; ++i) {
            removeKey(i);
        }
        assertEquals(0, allocatedDefaultAddressesOwnedByBPlusTree.size());
    }

    @Test
    public void testNoMemoryLeakOnInsertOOM() {
        int[] keys = new int[]{0, 1, 3, 4};
        for (int i = 0; i < keys.length; ++i) {
            insertKey(keys[i]);
        }

        for (int i = 0; i < keys.length; ++i) {
            assertHasKey(keys[i]);
        }

        Integer indexKey = 2;
        NativeMemoryData mapKeyData = toNativeData("Name_2");
        NativeMemoryData valueData = toNativeData("Value_2");

        try {
            throwKeyMemoryManagerOOMCounter = 1;
            btree.insert(indexKey, mapKeyData, valueData);
            fail("Should throw OOME");
        } catch (NativeOutOfMemoryError oome) {
            // no -op
        }
        for (int i = 0; i < keys.length; ++i) {
            assertHasKey(keys[i]);
        }

        assertEquals(keys.length, queryKeysCount());
    }

    private void assertHasKey(int index) {
        Iterator<Map.Entry<String, String>> it = btree.lookup(index);
        assertTrue(it.hasNext());
        Map.Entry<String, String> entry = it.next();
        assertEquals("Value_" + index, entry.getValue());
        assertFalse("" + index, it.hasNext());
    }

    class DelegatingDefaultMemoryAllocator implements HazelcastMemoryManager {

        private final HazelcastMemoryManager delegate;

        DelegatingDefaultMemoryAllocator(HazelcastMemoryManager memoryAllocator) {
            this.delegate = memoryAllocator;
        }

        @Override
        public long allocate(long size) {
            throwKeyMemoryManagerOOMCounter--;
            if (throwKeyMemoryManagerOOMCounter == 0) {
                throw new NativeOutOfMemoryError();
            }

            long address = delegate.allocate(size);
            allocatedDefaultAddressesOwnedByBPlusTree.add(address);
            return address;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            long newAddress = delegate.reallocate(address, currentSize, newSize);
            allocatedDefaultAddressesOwnedByBPlusTree.remove(address);
            allocatedDefaultAddressesOwnedByBPlusTree.add(newAddress);
            return newAddress;
        }

        @Override
        public void free(long address, long size) {
            delegate.free(address, size);
            allocatedDefaultAddressesOwnedByBPlusTree.remove(address);
            freedMemory = true;
        }

        @Override
        public MemoryAllocator getSystemAllocator() {
            return delegate.getSystemAllocator();
        }

        @Override
        public void compact() {
            delegate.compact();
        }

        @Override
        public boolean isDisposed() {
            return delegate.isDisposed();
        }

        @Override
        public MemoryStats getMemoryStats() {
            return delegate.getMemoryStats();
        }

        @Override
        public long getUsableSize(long address) {
            return delegate.getUsableSize(address);
        }

        @Override
        public long validateAndGetUsableSize(long address) {
            return delegate.validateAndGetUsableSize(address);
        }

        @Override
        public long getAllocatedSize(long address) {
            return delegate.getAllocatedSize(address);
        }

        @Override
        public long validateAndGetAllocatedSize(long address) {
            return delegate.validateAndGetAllocatedSize(address);
        }

        @Override
        public long newSequence() {
            return delegate.newSequence();
        }

        @Override
        public void dispose() {
            delegate.dispose();
        }
    }

    class IndexMemoryAllocatorCallback implements MemoryAllocatorEventCallback {

        @Override
        public void onAllocate(long addr, long size) {
            throwIndexMemoryManagerOOMCounter--;
            if (throwIndexMemoryManagerOOMCounter == 0) {
                throw new NativeOutOfMemoryError();
            }
            allocatedIndexAddresses.add(addr);
        }

        @Override
        public void onReallocate(long address, long currentSize, long newAddress, long newSize) {
            allocatedIndexAddresses.remove(address);
            allocatedIndexAddresses.add(newAddress);
        }

        @Override
        public void onFree(long address, long size) {
            allocatedIndexAddresses.remove(address);
            freedMemory = true;
        }

        @Override
        public void onDispose() {
            // no-op
        }
    }

}
