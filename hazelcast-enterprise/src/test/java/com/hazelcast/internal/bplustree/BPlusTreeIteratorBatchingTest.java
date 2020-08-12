package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.DelegatingLockManager;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.READ_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockManagerCallbackImpl;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeIteratorBatchingTest extends BPlusTreeTestSupport {

    private LockManager lockManager;

    private LockManagerCallbackImpl lockManagerCallback;

    @Before
    public void setUp() {
        lockManagerCallback = new LockManagerCallbackImpl();
        int stripesCount = nextPowerOfTwo(Runtime.getRuntime().availableProcessors() * 4);
        LockManager delegate = new HDLockManager(stripesCount);
        lockManager = new DelegatingLockManager(delegate, lockManagerCallback);
        super.setUp();
    }

    @Override
    LockManager newLockManager() {
        return lockManager;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @Override
    HDBPlusTree newBPlusTree(EnterpriseSerializationService ess,
                             MemoryAllocator keyAllocator,
                             MemoryAllocator indexAllocator, LockManager lockManager,
                             BPlusTreeKeyComparator keyComparator,
                             BPlusTreeKeyAccessor keyAccessor,
                             MapEntryFactory entryFactory,
                             int nodeSize,
                             int indexScanBatchSize) {
        return HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator, lockManager, keyComparator, keyAccessor,
                entryFactory, nodeSize, 5000);
    }

    int getNodeSize() {
        return 8192 * 4;
    }

    @Test
    public void testFullScanBatching() {
        insertKeysCompact(10000);
        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(7, getKeysCount(rootAddr));
        clearEventHistory();
        assertEquals(10000, queryKeysCount());
        assertEquals(10, leafNodeReadLockCountInHistory());
    }

    @Test
    public void testLookupBatching() {
        Integer indexKey = 10;
        int keysCount = 10000;

        // Insert entries with the same indexKey
        for (int i = 0; i < keysCount; ++i) {
            String mapKey = "Name_" + i;
            String value = "Value_" + i;
            NativeMemoryData mapKeyData = toNativeData(mapKey);
            NativeMemoryData valueData = toNativeData(value);
            MemoryBlock oldValue = btree.insert(indexKey, mapKeyData, valueData);
            assertNull(oldValue);
        }
        assertEquals(10000, queryKeysCount());
        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(13, getKeysCount(rootAddr));

        clearEventHistory();
        Iterator it = btree.lookup(10);
        assertIterator(keysCount, it);
        assertEquals(16, leafNodeReadLockCountInHistory());
    }

    private void clearEventHistory() {
        lockManagerCallback.eventsHistory.clear();
    }

    private int leafNodeReadLockCountInHistory() {
        int count = 0;
        for (BPlusTreeLockingTest.Triple triple : lockManagerCallback.eventsHistory) {
            // lockAddr is eqivalent to nodeAddr
            if (triple.eventType == READ_LOCK && getNodeLevel(triple.lockAddr) == 0) {
                count++;
            }
        }
        return count;
    }

    private void assertIterator(int expectedCount, Iterator it) {
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertEquals(expectedCount, count);
    }
}
