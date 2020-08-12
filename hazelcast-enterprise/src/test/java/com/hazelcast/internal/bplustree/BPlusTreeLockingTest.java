package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.INSTANT_WRITE_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.READ_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.RELEASE_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.TRY_UPGRADE_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.TRY_WRITE_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.LockEventType.WRITE_LOCK;
import static com.hazelcast.internal.bplustree.BPlusTreeLockingTest.Triple.create;
import static com.hazelcast.internal.bplustree.HDBPlusTree.DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeLockingTest extends BPlusTreeTestSupport {

    @Parameterized.Parameter
    public int indexScanBatchSize;

    @Parameterized.Parameters(name = "indexScanBatchSize: {0}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {0},
                {DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE},

        });
        // @formatter:on
    }

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
                             int indexScanBatchSize0) {
        return HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator, lockManager, keyComparator, keyAccessor,
                entryFactory, nodeSize, indexScanBatchSize);
    }

    @Test
    public void testLockCouplingOnIteration() {
        assumeTrue(indexScanBatchSize == 0);
        insertKeysCompact(9 * 9);

        assertEquals(1, getNodeLevel(rootAddr));
        clearEventHistory();

        // Check locks on lookup
        Iterator it = btree.lookup(null, true, null, true);

        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);

        List<Triple> expectedEvents = new ArrayList<>();
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));

        // Check navigation to the leaf history
        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);

        // Check locks on iteration
        List<Long> leafAddresses = getLeafAddresses();
        long currentNodeAddress = leafAddresses.get(0);
        long prevNodeAddress = 0;
        int currentLeafIndex = 0;
        int count = 1;
        // consume current element, no locks are needed
        it.next();

        while (it.hasNext()) {
            it.next();
            expectedEvents.add(create(READ_LOCK, currentNodeAddress));

            if (count % 9 == 0) {
                prevNodeAddress = currentNodeAddress;
                currentNodeAddress = leafAddresses.get(++currentLeafIndex);
                expectedEvents.add(create(READ_LOCK, currentNodeAddress));
                expectedEvents.add(create(RELEASE_LOCK, prevNodeAddress));
            }

            expectedEvents.add(create(RELEASE_LOCK, currentNodeAddress));
            count++;
        }

        // Add to the history an attempt to get next element on already exhausted iterator
        expectedEvents.add(create(READ_LOCK, currentNodeAddress));
        expectedEvents.add(create(RELEASE_LOCK, currentNodeAddress));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnIterationWithBatching() {
        assumeTrue(indexScanBatchSize > 0);
        insertKeysCompact(9 * 9);

        assertEquals(1, getNodeLevel(rootAddr));
        clearEventHistory();

        // Check locks on range scan
        Iterator it = btree.lookup(null, true, null, true);
        // exhaust the iterator
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertEquals(81, count);

        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);

        List<Triple> expectedEvents = new ArrayList<>();
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));

        // Check locks on iteration
        List<Long> leafAddresses = getLeafAddresses();
        long lastLeafAddr = leafAddresses.get(leafAddresses.size() - 1);

        long prevLeafAddr = NULL_ADDRESS;
        for (long leafAddr : leafAddresses) {
            expectedEvents.add(create(READ_LOCK, leafAddr));

            if (prevLeafAddr != NULL_ADDRESS) {
                expectedEvents.add(create(RELEASE_LOCK, prevLeafAddr));
            }
            prevLeafAddr = leafAddr;
        }
        // Release lock on the last leaf
        expectedEvents.add(create(RELEASE_LOCK, lastLeafAddr));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }


    @Test
    public void testLockCouplingOnInsertNoSplit() {
        insertKeys(5);
        assertEquals(1, getNodeLevel(rootAddr));
        clearEventHistory();

        insertKey(6);

        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        List<Triple> expectedEvents = new ArrayList<>();
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnLeafSplit() {
        insertKeys(9);
        assertEquals(1, getNodeLevel(rootAddr));
        clearEventHistory();

        // Overflow the leaf, it triggers its split
        insertKey(10);

        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        long newChild = innerNodeAccessor.getValueAddr(rootAddr, 1);
        List<Triple> expectedEvents = new ArrayList<>();
        // Split part
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftMostChild));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, newChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, newChild));

        long rightMostChild = innerNodeAccessor.getValueAddr(rootAddr, 1);
        // Actual insertion part
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, rightMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, rightMostChild));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnCascadingSplit() {
        insertKeysCompact(9 * 9);
        assertEquals(1, getNodeLevel(rootAddr));
        clearEventHistory();

        long oldRightMostChild = innerNodeAccessor.getValueAddr(rootAddr, getKeysCount(rootAddr));

        // Overflow the rightmost leaf, it triggers a cascading split
        insertKey(82);

        // Failed attempt to insert key into the rightmost child
        List<Triple> expectedEvents = new ArrayList<>();
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, oldRightMostChild));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, oldRightMostChild));

        // Failed attempt to split rightmost leaf, root is full
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, oldRightMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, oldRightMostChild));

        long rightMostChild = innerNodeAccessor.getValueAddr(rootAddr, 1);

        // Increment depth of the BTree, noticed that root should be splitted after
        // it's been read locked, and this is why we re-acquire the lock.
        long leftInnerChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftInnerChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftInnerChild));

        // Now, split the inner node (content of the former root node)
        long newInnerChild = innerNodeAccessor.getValueAddr(rootAddr, 1);
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftInnerChild));
        expectedEvents.add(create(WRITE_LOCK, newInnerChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftInnerChild));
        expectedEvents.add(create(RELEASE_LOCK, newInnerChild));

        // Split node on the level = 1, there is space for the split key
        rightMostChild = innerNodeAccessor.getValueAddr(rootAddr, 1);
        long leaf = innerNodeAccessor.getValueAddr(rightMostChild, getKeysCount(rightMostChild) - 1);
        long newLeaf = innerNodeAccessor.getValueAddr(rightMostChild, getKeysCount(rightMostChild));

        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, rightMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(WRITE_LOCK, newLeaf));
        expectedEvents.add(create(RELEASE_LOCK, rightMostChild));
        expectedEvents.add(create(RELEASE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, newLeaf));

        // Now, we are ready to insert the key
        leaf = innerNodeAccessor.getValueAddr(rightMostChild, getKeysCount(rightMostChild));
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, rightMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, rightMostChild));
        expectedEvents.add(create(RELEASE_LOCK, leaf));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnRemoveNoEmptyNode() {
        insertKeysCompact(9 * 9);
        assertEquals(1, getNodeLevel(rootAddr));
        clearEventHistory();

        removeKey(0);

        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        List<Triple> expectedEvents = new ArrayList<>();
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnRemoveMakingNodeEmpty() {
        insertKeysCompact(9 * 9);
        assertEquals(1, getNodeLevel(rootAddr));

        // Leave only one key on the leftmost leaf
        for (int i = 0; i < 8; ++i) {
            removeKey(i);
        }

        clearEventHistory();

        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);

        // Remove the last key on the leaf, the leaf should be removed
        removeKey(8);

        List<Triple> expectedEvents = new ArrayList<>();

        // Remove the last key on the leaf
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));

        // Remove an empty node from the BTree
        long newLeftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leftMostChild));
        expectedEvents.add(create(WRITE_LOCK, newLeftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, newLeftMostChild));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);

        // Leave only one key on the node in the middle of the chain
        for (int i = 18; i < 26; ++i) {
            removeKey(i);
        }
        clearEventHistory();

        long leaf = innerNodeAccessor.getValueAddr(rootAddr, 1);
        long rightChild = innerNodeAccessor.getValueAddr(rootAddr, 2);
        // Remove the last key on the node
        removeKey(26);
        leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);

        expectedEvents.clear();
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leaf));

        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(TRY_WRITE_LOCK, leftMostChild));
        expectedEvents.add(create(WRITE_LOCK, rightChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, leftMostChild));
        expectedEvents.add(create(RELEASE_LOCK, rightChild));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);

        // Now, delete the rightmost leaf

        // First, leave only one key on it
        for (int i = 72; i < 80; ++i) {
            removeKey(i);
        }
        clearEventHistory();

        leaf = innerNodeAccessor.getValueAddr(rootAddr, getKeysCount(rootAddr));
        long leftChild = innerNodeAccessor.getValueAddr(rootAddr, getKeysCount(rootAddr) - 1);
        // Remove the last key
        removeKey(80);

        expectedEvents.clear();

        // Remove the last key on the leaf
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leaf));

        // Remove an empty node from the BTree
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(TRY_WRITE_LOCK, leftChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, leftChild));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnMakingTreeEmpty() {
        insertKeysCompact(9 * 10);
        assertEquals(2, getNodeLevel(rootAddr));

        for (int i = 1; i < 90; ++i) {
            removeKey(i);
        }
        clearEventHistory();

        // Remove the last key
        removeKey(0);

        assertEquals(2, getNodeLevel(rootAddr));

        List<Triple> expectedEvents = new ArrayList<>();

        // Trace for actual deletion of the key
        long innerChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        long leaf = innerNodeAccessor.getValueAddr(innerChild, 0);
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, innerChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, innerChild));
        expectedEvents.add(create(RELEASE_LOCK, leaf));

        // Try to make actual subtree deletion
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, innerChild));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, innerChild));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, innerChild));
        expectedEvents.add(create(RELEASE_LOCK, leaf));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    @Test
    public void testLockCouplingOnRemoveSubtree() {
        insertKeysCompact(9 * 10);
        assertEquals(2, getNodeLevel(rootAddr));
        assertEquals(1, getKeysCount(rootAddr));

        int splitKey = ess.toObject(innerNodeAccessor.getIndexKeyHeapData(rootAddr, 0));
        // Remove all (except one) keys on the right sub-tree
        for (int i = splitKey + 2; i < 90; ++i) {
            removeKey(i);
        }

        clearEventHistory();

        long rightInnerChild = innerNodeAccessor.getValueAddr(rootAddr, 1);
        long leaf = innerNodeAccessor.getValueAddr(rightInnerChild, 0);
        long leafLeftNeighbour = leafNodeAccessor.getBackNode(leaf);

        // Remove the last key of the right sub-tree
        removeKey(splitKey + 1);

        assertEquals(0, getKeysCount(rootAddr));

        List<Triple> expectedEvents = new ArrayList<>();

        // Trace removal of the last key of the right sub-tree
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, rightInnerChild));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, rightInnerChild));
        expectedEvents.add(create(RELEASE_LOCK, leaf));

        // Trace right sub-tree removal
        expectedEvents.add(create(READ_LOCK, rootAddr));
        expectedEvents.add(create(READ_LOCK, rightInnerChild));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rootAddr));
        expectedEvents.add(create(TRY_UPGRADE_LOCK, rightInnerChild));
        expectedEvents.add(create(WRITE_LOCK, leaf));
        expectedEvents.add(create(TRY_WRITE_LOCK, leafLeftNeighbour));
        expectedEvents.add(create(RELEASE_LOCK, rootAddr));
        expectedEvents.add(create(RELEASE_LOCK, rightInnerChild));
        expectedEvents.add(create(RELEASE_LOCK, leaf));
        expectedEvents.add(create(RELEASE_LOCK, leafLeftNeighbour));

        assertEquals(expectedEvents, lockManagerCallback.eventsHistory);
    }

    enum LockEventType {
        READ_LOCK,
        WRITE_LOCK,
        TRY_UPGRADE_LOCK,
        TRY_WRITE_LOCK,
        INSTANT_WRITE_LOCK,
        RELEASE_LOCK
    }

    interface LockManagerCallback {

        void onReadLock(long lockAddr);

        void onWriteLock(long lockAddr);

        void onTryUpgradeLock(long lockAddr, boolean result);

        void onTryWriteLock(long lockAddr, boolean result);

        void onInstantWriteLock(long lockAddr);

        void onReleaseLock(long lockAddr);
    }

    static class LockManagerCallbackImpl implements LockManagerCallback {

        final List<Triple> eventsHistory = new ArrayList<>();

        @Override
        public void onReadLock(long lockAddr) {
            Triple triple = new Triple(READ_LOCK, lockAddr, true);
            eventsHistory.add(triple);
        }

        @Override
        public void onWriteLock(long lockAddr) {
            Triple triple = new Triple(WRITE_LOCK, lockAddr, true);
            eventsHistory.add(triple);
        }

        @Override
        public void onTryUpgradeLock(long lockAddr, boolean result) {
            Triple triple = new Triple(TRY_UPGRADE_LOCK, lockAddr, result);
            eventsHistory.add(triple);
        }

        @Override
        public void onTryWriteLock(long lockAddr, boolean result) {
            Triple triple = new Triple(TRY_WRITE_LOCK, lockAddr, result);
            eventsHistory.add(triple);
        }

        @Override
        public void onInstantWriteLock(long lockAddr) {
            Triple triple = new Triple(INSTANT_WRITE_LOCK, lockAddr, true);
            eventsHistory.add(triple);
        }

        @Override
        public void onReleaseLock(long lockAddr) {
            Triple triple = new Triple(RELEASE_LOCK, lockAddr, true);
            eventsHistory.add(triple);
        }
    }

    static class DelegatingLockManager implements LockManager {

        private final LockManager delegate;
        private final LockManagerCallback callback;

        DelegatingLockManager(LockManager delegate, LockManagerCallback callback) {
            this.delegate = delegate;
            this.callback = callback;
        }

        @Override
        public void readLock(long lockAddr) {
            delegate.readLock(lockAddr);
            callback.onReadLock(lockAddr);
        }

        @Override
        public void writeLock(long lockAddr) {
            delegate.writeLock(lockAddr);
            callback.onWriteLock(lockAddr);
        }

        @Override
        public boolean tryUpgradeToWriteLock(long lockAddr) {
            boolean result = delegate.tryUpgradeToWriteLock(lockAddr);
            callback.onTryUpgradeLock(lockAddr, result);
            return result;
        }

        @Override
        public boolean tryWriteLock(long lockAddr) {
            boolean result = delegate.tryWriteLock(lockAddr);
            callback.onTryWriteLock(lockAddr, result);
            return result;
        }

        @Override
        public void instantDurationWriteLock(long lockAddr) {
            delegate.instantDurationWriteLock(lockAddr);
            callback.onInstantWriteLock(lockAddr);
        }

        @Override
        public void releaseLock(long lockAddr) {
            delegate.releaseLock(lockAddr);
            callback.onReleaseLock(lockAddr);
        }
    }

    static final class Triple {
        final LockEventType eventType;
        final long lockAddr;
        final boolean result;

        static Triple create(LockEventType eventType, long lockAddr) {
            return create(eventType, lockAddr, true);
        }

        static Triple create(LockEventType eventType, long lockAddr, boolean result) {
            Triple triple = new Triple(eventType, lockAddr, result);
            return triple;
        }

        private Triple(LockEventType eventType, long lockAddr, boolean result) {
            this.eventType = eventType;
            this.lockAddr = lockAddr;
            this.result = result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Triple that = (Triple) o;

            return eventType == that.eventType && lockAddr == that.lockAddr && result == that.result;
        }

        @Override
        public int hashCode() {
            int hashCode = eventType.hashCode();
            hashCode = 31 * hashCode + Long.hashCode(lockAddr);
            hashCode = 31 * hashCode + (result ? 1 : 0);
            return hashCode;
        }

        @Override
        public String toString() {
            return "Triple("
                    + "eventType=" + eventType
                    + ", lockAddr=" + lockAddr
                    + ", result=" + result
                    + ')';
        }
    }

    private void clearEventHistory() {
        lockManagerCallback.eventsHistory.clear();
    }

    private List<Long> getLeafAddresses() {
        List<Long> result = new ArrayList<>();
        long leftMostChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        assertOnLeafNodes(leftMostChildAddr, nodeAddr -> {
            result.add(nodeAddr);
        });
        return result;
    }
}
