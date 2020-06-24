package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeMemoryTest extends BPlusTreeTestSupport {

    private Set<Long> allocatedDefaultAddressesOwnedByBPlusTree = new HashSet<>();
    private Set<Long> allocatedIndexAddresses = new HashSet<>();
    private int throwOOMCounter = Integer.MAX_VALUE;
    private boolean freedMemory;

    @Override
    HazelcastMemoryManager newDefaultMemoryAllocator() {
        HazelcastMemoryManager defaultAllocator = new StandardMemoryManager(new MemorySize(1300, MemoryUnit.MEGABYTES));
        return new DelegatingDefaultMemoryAllocator(defaultAllocator);
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
        throwOOMCounter = 2;
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
        throwOOMCounter = 1;
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

    class DelegatingDefaultMemoryAllocator implements HazelcastMemoryManager {

        private final HazelcastMemoryManager delegate;

        DelegatingDefaultMemoryAllocator(HazelcastMemoryManager memoryAllocator) {
            this.delegate = memoryAllocator;
        }

        @Override
        public long allocate(long size) {
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
            throwOOMCounter--;
            if (throwOOMCounter == 0) {
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
