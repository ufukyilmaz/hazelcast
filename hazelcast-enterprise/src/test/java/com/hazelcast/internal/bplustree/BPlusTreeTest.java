package com.hazelcast.internal.bplustree;

import com.hazelcast.config.IndexType;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.IndexType.HASH;
import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.internal.bplustree.HDBPlusTree.DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.OFFSET_NODE_BASE_DATA;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getSequenceNumber;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.serialization.DataType.NATIVE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeTest extends BPlusTreeTestSupport {

    @Parameterized.Parameter
    public int indexScanBatchSize;

    @Parameterized.Parameter(1)
    public IndexType indexType;

    @Parameterized.Parameters(name = "indexScanBatchSize: {0} indexType: {1}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {0, SORTED}, // batching is disabled
                {DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE, SORTED},
                {0, HASH},
                {DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE, HASH},
        });
        // @formatter:on
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    @Override
    HDBPlusTree newBPlusTree(EnterpriseSerializationService ess,
                             MemoryAllocator keyAllocator,
                             MemoryAllocator indexAllocator, LockManager lockManager,
                             BPlusTreeKeyComparator keyComparator0,
                             BPlusTreeKeyAccessor keyAccessor,
                             MapEntryFactory entryFactory,
                             int nodeSize,
                             int indexScanBatchSize0,
                             EntrySlotPayload entrySlotPayload0) {
        BPlusTreeKeyComparator keyComparator = newBPlusTreeKeyComparator();
        EntrySlotPayload entrySlotPayload = newEntrySlotPayload();
        return HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator, lockManager, keyComparator, keyAccessor,
                entryFactory, nodeSize, indexScanBatchSize, entrySlotPayload);
    }

    @Override
    BPlusTreeKeyComparator newBPlusTreeKeyComparator() {
        return indexType == HASH ? new HashIndexBPlusTreeKeyComparator(ess)
                : new DefaultBPlusTreeKeyComparator(ess);
    }

    @Override
    EntrySlotPayload newEntrySlotPayload() {
        return indexType == HASH ? new HashIndexEntrySlotPayload()
                : new EntrySlotNoPayload();
    }

    int getSlotSize() {
        return indexType == HASH ? 32 : 24;
    }

    int getLeafSlotsCount() {
        int slotSize = getSlotSize();
        return (getNodeSize() - OFFSET_NODE_BASE_DATA - 16) / slotSize;
    }

    int getInnerSlotsCount() {
        int slotSize = getSlotSize();
        return (getNodeSize() - OFFSET_NODE_BASE_DATA) / slotSize;
    }

    @Test
    public void testInsertNoSplit() {
        for (int i = 0; i < 9; ++i) {
            Integer indexKey = i;
            String mapKey = "Name_" + i;
            String value = "Value_" + i;
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            MemoryBlock oldValue = btree.insert(indexKey, mapKeyData, valueData);
            assertNull(oldValue);
            Iterator<Map.Entry> it = btree.lookup(i);
            assertTrue(it.hasNext());
            Map.Entry entry = it.next();
            assertEquals(mapKey, entry.getKey());
            assertEquals(value, entry.getValue());
            assertFalse(it.hasNext());
        }

        assertEquals(9, queryKeysCount());
        assertEquals(1, getNodeLevel(rootAddr));
    }

    @Test
    public void testInsertNonUniqueIndexKeys() {
        Integer indexKey = 10;

        int keysCount = getLeafSlotsCount();

        for (int i = 0; i < keysCount; ++i) {
            String mapKey = "Name_" + i;
            String value = "Value_" + i;
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            MemoryBlock oldValue = btree.insert(indexKey, mapKeyData, valueData);
            assertNull(oldValue);
            long child = innerNodeAccessor.getValueAddr(rootAddr, 0);
            assertNotEquals(NULL_ADDRESS, child);
            assertEquals(i + 1, getKeysCount(child));
            assertEquals(indexKey, ess.toObject(leafNodeAccessor.getIndexKeyHeapData(child, i)));
            assertEquals(mapKey, ess.toObject(leafNodeAccessor.getEntryKey(child, i)));
            assertEquals(value, ess.toObject(leafNodeAccessor.getValue(child, i)));
            assertIteratorCount(i + 1, indexKey);
        }
        assertEquals(keysCount, queryKeysCount());
        assertEquals(1, getNodeLevel(rootAddr));
    }

    @Test
    public void testInsertDuplicates() {
        for (int i = 0; i < 81; ++i) {
            insertKey(i);
        }

        for (int i = 0; i < 81; ++i) {
            insertKey(i);
        }

        assertEquals(81, queryKeysCount());
    }

    @Test
    public void testCompactInsert() {
        assumeTrue(indexType == SORTED);
        btree.setNodeSplitStrategy(new EmptyNewNodeSplitStrategy());
        for (int i = 0; i < 9 * 9; ++i) {
            Integer indexKey = i;
            String mapKey = "Name_" + i;
            String value = "Value_" + i;
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            btree.insert(indexKey, mapKeyData, valueData);
        }

        assertEquals(1, getNodeLevel(rootAddr));
        assertTrue(innerNodeAccessor.isNodeFull(rootAddr));
        long leftMostChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        assertNotEquals(NULL_ADDRESS, leftMostChildAddr);

        MutableInteger leafCount = new MutableInteger();
        assertOnLeafNodes(leftMostChildAddr, nodeAddr -> {
            assertTrue(leafNodeAccessor.isNodeFull(nodeAddr));
            leafCount.getAndInc();
        });
        assertEquals(9, leafCount.value);
    }

    @Test
    public void testReplaceKeys() {
        insertKeys(9);

        assertEquals(9, queryKeysCount());

        for (int i = 0; i < 9; ++i) {
            Integer indexKey = i;
            String mapKey = "Name_" + i;
            String value = "Value_" + i + "_" + i;
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            MemoryBlock oldValue = btree.insert(indexKey, mapKeyData, valueData);
            assertNotNull(oldValue);
            assertEquals("Value_" + i, ess.toObject(oldValue));
            Iterator<Map.Entry> it = btree.lookup(i);
            assertTrue(it.hasNext());
            Map.Entry entry = it.next();
            assertEquals(mapKey, entry.getKey());
            assertEquals(value, entry.getValue());
            assertFalse(it.hasNext());
        }

        assertEquals(9, queryKeysCount());
    }

    @Test
    public void testLeafSplit() {
        int keysCount = getLeafSlotsCount() + 1;
        // overflow a leaf page to make a split
        insertKeys(keysCount);

        // test root node
        assertEquals(1, getKeysCount(rootAddr));
        assertEquals(1, getNodeLevel(rootAddr));
        Data splitIndexKey = innerNodeAccessor.getIndexKeyHeapData(rootAddr, 0);
        NativeMemoryData splitMapKey = innerNodeAccessor.getEntryKey(rootAddr, 0);
        // For the HASH index the split key depends on the hash value
        Integer expectedSplitIndexKey = indexType == SORTED ? 3 : 1;
        assertEquals(expectedSplitIndexKey, ess.toObject(splitIndexKey));
        assertEquals("Name_" + expectedSplitIndexKey, ess.toObject(splitMapKey));

        assertEquals(NULL_ADDRESS, innerNodeAccessor.getIndexKeyAddr(rootAddr, 1));
        assertEquals(NULL_ADDRESS, innerNodeAccessor.getEntryKeyAddr(rootAddr, 1));

        long leftChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        long rightChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 1);
        assertNotEquals(NULL_ADDRESS, leftChildAddr);
        assertNotEquals(NULL_ADDRESS, rightChildAddr);

        // test left child
        if (indexType == SORTED) {
            assertLeafSlotValues(leftChildAddr, 0, 3);
            assertKeysSorted(leftChildAddr, leafNodeAccessor);
        }
        assertEquals(NULL_ADDRESS, leafNodeAccessor.getBackNode(leftChildAddr));
        assertEquals(rightChildAddr, leafNodeAccessor.getForwNode(leftChildAddr));

        // test right child
        if (indexType == SORTED) {
            assertLeafSlotValues(rightChildAddr, 4, 9);
            assertKeysSorted(rightChildAddr, leafNodeAccessor);
        }
        assertEquals(leftChildAddr, leafNodeAccessor.getBackNode(rightChildAddr));
        assertEquals(NULL_ADDRESS, leafNodeAccessor.getForwNode(rightChildAddr));

        int leftIndexKey = ess.toObject(leafNodeAccessor.getIndexKeyHeapData(leftChildAddr, 0));
        if (indexType == SORTED) {
            assertFromKeyIteratorCount(keysCount, leftIndexKey);
        } else {
            assertEquals(keysCount, queryKeysCount());
        }
    }


    @Test
    public void testIncrementDepth() {
        // Fill in the 1 level B+tree
        int keysCount = getLeafSlotsCount() * getInnerSlotsCount();
        insertKeysCompact(keysCount, indexType);

        assertNestedKeysSorted(rootAddr, 0, keysCount, 1, indexType == HASH);

        assertEquals(getInnerSlotsCount() - 1, getKeysCount(rootAddr));
        assertEquals(1, getNodeLevel(rootAddr));
        assertTrue(innerNodeAccessor.isNodeFull(rootAddr));
        if (indexType == SORTED) {
            assertKeysSorted(rootAddr, innerNodeAccessor);
        } else {
            assertKeysSortedByHash(rootAddr, innerNodeAccessor);
        }

        assertEquals(keysCount, queryKeysCount());

        // assert all leaf pages are full
        long leftMostChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        assertOnLeafNodes(leftMostChild, nodeAddr -> assertTrue(leafNodeAccessor.isNodeFull(nodeAddr)));

        // overflow the last leaf (for SORTED index), causing the root split and depth increment
        insertKey(82);

        assertNestedKeysSorted(rootAddr, 0, 81, 2, indexType == HASH);

        // test root node
        assertEquals(2, getNodeLevel(rootAddr));
        assertEquals(1, getKeysCount(rootAddr));

        Data splitIndexKey = innerNodeAccessor.getIndexKeyHeapData(rootAddr, 0);
        NativeMemoryData splitMapKey = innerNodeAccessor.getEntryKey(rootAddr, 0);

        if (indexType == SORTED) {
            Integer expectedSplitIndexKey = 35;
            assertEquals(expectedSplitIndexKey, ess.toObject(splitIndexKey));
            assertEquals("Name_" + expectedSplitIndexKey, ess.toObject(splitMapKey));
        }

        assertEquals(NULL_ADDRESS, innerNodeAccessor.getIndexKeyAddr(rootAddr, 1));
        assertEquals(NULL_ADDRESS, innerNodeAccessor.getEntryKeyAddr(rootAddr, 1));

        // test the 1st level inner nodes
        long leftInnerChild = innerNodeAccessor.getValueAddr(rootAddr, 0);
        long rightInnerChild = innerNodeAccessor.getValueAddr(rootAddr, 1);
        assertNotEquals(NULL_ADDRESS, leftInnerChild);
        assertNotEquals(NULL_ADDRESS, rightInnerChild);
        assertEquals(1, getNodeLevel(leftInnerChild));
        assertEquals(1, getNodeLevel(rightInnerChild));
        assertEquals(3, getKeysCount(leftInnerChild));
        // We expect 5 keys on the rightInnerChild, because inner split will create a new inner
        // node with 4 keys, but insertion of the key will add one extra split key
        int rightInnerChildKeysCount = indexType == SORTED ? 5 : 3;
        assertEquals(rightInnerChildKeysCount, getKeysCount(rightInnerChild));

        leftMostChild = innerNodeAccessor.getValueAddr(leftInnerChild, 0);
        assertNotEquals(NULL_ADDRESS, leftMostChild);
        MutableInteger currentNode = new MutableInteger();

        if (indexType == SORTED) {
            // test the leaf nodes
            assertOnLeafNodes(leftMostChild,
                    nodeAddr -> {
                        if (currentNode.value < getInnerSlotsCount() - 1) {
                            assertTrue(String.valueOf(currentNode.value), leafNodeAccessor.isNodeFull(nodeAddr));
                        } else if (currentNode.value == 8) {
                            assertEquals(4, getKeysCount(nodeAddr));
                        } else {
                            assertEquals(6, getKeysCount(nodeAddr));
                            assertEquals(NULL_ADDRESS, leafNodeAccessor.getForwNode(nodeAddr));
                        }
                        currentNode.getAndInc();
                    });
            assertEquals(10, currentNode.value);
        } else {
            // For the HASH index not necessarily the left most node was splitted
            MutableInteger countFullNodes = new MutableInteger();
            assertOnLeafNodes(leftMostChild,
                    nodeAddr -> {
                        if (leafNodeAccessor.isNodeFull(nodeAddr)) {
                            countFullNodes.getAndInc();
                        }
                        currentNode.getAndInc();
                    });
            assertEquals(getInnerSlotsCount() - 1, countFullNodes.value);
            assertEquals(getInnerSlotsCount() + 1, currentNode.value);
        }
    }

    @Test
    public void testRemove() {
        int keysCount = getLeafSlotsCount();
        insertKeys(keysCount);
        allocatorCallback.clear();
        assertEquals(1, getNodeLevel(rootAddr));
        long leafAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        assertEquals(keysCount, getKeysCount(leafAddr));
        long seqCount = getSequenceNumber(leafAddr);

        // Remove key from the middle
        Integer indexKey = 5;
        String mapKey = "Name_" + 5;
        NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
        NativeMemoryData oldValue = btree.remove(indexKey, mapKeyData);
        assertNotNull(oldValue);
        assertEquals("Value_5", ess.toObject(oldValue));
        assertKeysCountAndSeqCount(leafAddr, keysCount - 1, ++seqCount);

        // Remove edge keys
        oldValue = btree.remove(0, ess.toData("Name_0", NATIVE));
        assertNotNull(oldValue);
        assertEquals("Value_0", ess.toObject(oldValue));
        assertKeysCountAndSeqCount(leafAddr, keysCount - 2, ++seqCount);

        int removeIndexKey = keysCount - 3;
        oldValue = btree.remove(removeIndexKey, ess.toData("Name_" + removeIndexKey, NATIVE));
        assertNotNull(oldValue);
        assertEquals("Value_" + removeIndexKey, ess.toObject(oldValue));
        assertKeysCountAndSeqCount(leafAddr, keysCount - 3, ++seqCount);

        // Remove not existing key
        oldValue = btree.remove(100, ess.toData("Name_3", NATIVE));
        assertNull(oldValue);
        assertKeysCountAndSeqCount(leafAddr, keysCount - 3, seqCount);

        oldValue = btree.remove(2, ess.toData("Name_100", NATIVE));
        assertNull(oldValue);
        assertKeysCountAndSeqCount(leafAddr, keysCount - 3, seqCount);

        assertFalse(maAllocateAddr.hasUpdates());
        assertFalse(maFreeAddr.hasUpdates());
    }

    @Test
    public void testRemoveLeafNode() {
        assumeTrue(indexType == SORTED);
        insertKeysCompact(9 * 3);
        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(2, getKeysCount(rootAddr));

        long leftLeafAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        long midLeafAddr = innerNodeAccessor.getValueAddr(rootAddr, 1);
        long rightLeafAddr = innerNodeAccessor.getValueAddr(rootAddr, 2);
        long leftLeafSeqCount = getSequenceNumber(leftLeafAddr);
        long rightLeafSeqCount = getSequenceNumber(rightLeafAddr);

        // Remove all keys from the mid child causing its remove
        for (int i = 9; i < 18; ++i) {
            btree.remove(i, nativeData("Name_" + i));
        }
        assertEquals(midLeafAddr, maFreeAddr.get());

        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(1, getKeysCount(rootAddr));
        assertEquals(leftLeafAddr, innerNodeAccessor.getValueAddr(rootAddr, 0));
        assertEquals(rightLeafAddr, innerNodeAccessor.getValueAddr(rootAddr, 1));
        assertEquals(NULL_ADDRESS, leafNodeAccessor.getBackNode(leftLeafAddr));
        assertEquals(rightLeafAddr, leafNodeAccessor.getForwNode(leftLeafAddr));
        assertEquals(leftLeafAddr, leafNodeAccessor.getBackNode(rightLeafAddr));
        assertEquals(NULL_ADDRESS, leafNodeAccessor.getForwNode(rightLeafAddr));

        assertEquals(18, queryKeysCount());
        assertKeysCountAndSeqCount(rightLeafAddr, 9, ++rightLeafSeqCount);
        assertKeysCountAndSeqCount(leftLeafAddr, 9, ++leftLeafSeqCount);

        // Remove all keys from the left child causing left child remove
        for (int i = 0; i < 9; ++i) {
            btree.remove(i, nativeData("Name_" + i));
        }
        assertEquals(leftLeafAddr, maFreeAddr.get());

        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(0, getKeysCount(rootAddr));
        assertEquals(rightLeafAddr, innerNodeAccessor.getValueAddr(rootAddr, 0));
        assertKeysCountAndSeqCount(rightLeafAddr, 9, ++rightLeafSeqCount);
        assertEquals(NULL_ADDRESS, leafNodeAccessor.getBackNode(rightLeafAddr));
        assertEquals(NULL_ADDRESS, leafNodeAccessor.getForwNode(rightLeafAddr));

        allocatorCallback.clear();
        // Remove all keys from the only leaf, the leaf should stay
        for (int i = 18; i < 27; ++i) {
            btree.remove(i, nativeData("Name_" + i));
        }
        assertFalse(maFreeAddr.hasUpdates());

        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(0, getKeysCount(rootAddr));
        assertEquals(rightLeafAddr, innerNodeAccessor.getValueAddr(rootAddr, 0));
        assertKeysCountAndSeqCount(rightLeafAddr, 0, rightLeafSeqCount + 9);
    }

    @Test
    public void testRemoveInnerNode() {
        assumeTrue(indexType == SORTED);
        // Fill in completely the 1 level B+tree
        int keysCount = getLeafSlotsCount() * getInnerSlotsCount();
        insertKeysCompact(keysCount, indexType);

        // Overflow the last leaf node with the default split strategy
        insertKey(keysCount);
        assertEquals(2, getNodeLevel(rootAddr));
        assertEquals(1, getKeysCount(rootAddr));

        int rootSplitKey = ess.toObject(innerNodeAccessor.getIndexKeyHeapData(rootAddr, 0));
        int expectedRootSplitKey = 35;
        assertEquals(expectedRootSplitKey, rootSplitKey);

        long leftChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        List<Long> descendants = getChildrenAddrs(leftChildAddr);
        descendants.add(leftChildAddr);
        assertEquals(5, descendants.size());

        long rightChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 1);
        descendants = getChildrenAddrs(rightChildAddr);
        descendants.add(rightChildAddr);
        assertEquals(7, descendants.size());

        allocatorCallback.clear();
        // Remove all keys from the right sub-tree causing its removal
        for (int i = 36; i <= 81; ++i) {
            assertNotNull(btree.remove(i, nativeData("Name_" + i)));
        }

        assertTrue(CollectionUtils.isEqualCollection(descendants, maFreeAddr.versions()));
        assertFalse(maAllocateAddr.hasUpdates());

        assertEquals(2, getNodeLevel(rootAddr));
        assertEquals(0, getKeysCount(rootAddr));
        assertEquals(36, queryKeysCount());
    }

    @Test
    public void testRemoveMultipleKeys() {
        int keysCount = getLeafSlotsCount() * (getInnerSlotsCount()) + 1;
        insertKeysCompact(keysCount, indexType);
        assertEquals(2, getNodeLevel(rootAddr));

        for (int i = 1; i < keysCount; ++i) {
            removeKey(i);
            assertFalse(btree.lookup(i).hasNext());
        }
        assertEquals(1, queryKeysCount());
    }

    @Test
    public void testForwBackLinks() {
        // Fill in completely the 1 level B+tree
        int keysCount = getLeafSlotsCount() * getInnerSlotsCount();
        insertKeysCompact(keysCount, indexType);
        assertEquals(1, getNodeLevel(rootAddr));
        assertEquals(getInnerSlotsCount() - 1, getKeysCount(rootAddr));

        long leftMostChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        AtomicInteger count = new AtomicInteger();

        // Check forward links
        assertOnLeafNodes(leftMostChildAddr, false, leafAddr -> {
            assertTrue(leafNodeAccessor.isNodeFull(leafAddr));
            count.incrementAndGet();
        });
        assertEquals(getInnerSlotsCount(), count.get());

        // check backward links
        count.set(0);
        long rightMostChildAddr = innerNodeAccessor.getValueAddr(rootAddr, getInnerSlotsCount() - 1);
        assertOnLeafNodes(rightMostChildAddr, true, leafAddr -> {
            assertTrue(leafNodeAccessor.isNodeFull(leafAddr));
            count.incrementAndGet();
        });
        assertEquals(getInnerSlotsCount(), count.get());
    }

    @Test
    public void testLookup() {
        insertKeys(10000);
        assertEquals(10000, queryKeysCount());

        for (int i = 0; i < 100; ++i) {
            int key = nextInt(10000);
            Iterator<Map.Entry> it = btree.lookup(key);
            assertIterator(it, 1, key);
        }
    }

    @Test
    public void testRangeLookup() {
        assumeTrue(indexType == SORTED);
        insertKeys(10000);

        int key = nextInt(9997);

        Iterator<Map.Entry> it = btree.lookup(key, true, key + 1, false);
        assertIterator(it, 1, key);

        it = btree.lookup(key, true, key, false);
        assertIterator(it, 0, key);

        it = btree.lookup(key, true, key + 1, true);
        assertIterator(it, 2, key);

        it = btree.lookup(key, false, key + 1, false);
        assertIterator(it, 0, key);

        it = btree.lookup(-1, true, 0, false);
        assertIterator(it, 0, 0);

        it = btree.lookup(-10, true, 5, false);
        assertIterator(it, 5, 0);

        it = btree.lookup(key, true, key + 5, true);
        assertIterator(it, 6, key);

        it = btree.lookup(5, true, 3, true);
        assertIterator(it, 0, key);

        it = btree.lookup(1000, true, 2000, true);
        assertIterator(it, 1001, 1000);
    }

    @Test
    public void testRangeScanOnNodeBoundary() {
        assumeTrue(indexType == SORTED);

        insertKeysCompact(10);

        // Make sure the last result from the iterator is a last slot in the B+tree node
        Iterator it = btree.lookup(0, true, 8, true);
        while (it.hasNext()) {
            it.next();
        }

        // Call hasNext() on already exhausted iterator
        assertFalse(it.hasNext());
    }

    @Test
    public void testHasNextOnExhaustedIterator() {
        assumeTrue(indexType == SORTED);

        int keysCount = 10000;
        insertKeys(keysCount);

        for (int i = 0; i < 100; ++i) {
            int from = nextInt(keysCount);
            int to = from + 1000;
            Iterator it = btree.lookup(from, true, to, true);

            int expectedCount = Math.min(to, keysCount - 1) - from + 1;

            assertIterator(it, expectedCount, from);

            assertFalse(it.hasNext());
        }
    }

    @Test
    public void testFullScan() {
        // All keys fit into one node
        insertKeys(9);
        assertEquals(9, queryKeysCount());

        // Keys span multiple leaf nodes
        insertKeys(100);
        assertEquals(100, queryKeysCount());
    }

    @Test
    public void testLookupNotExistingKey() {
        insertKeys(100);

        Iterator it = btree.lookup(101);
        assertFalse(it.hasNext());

        it = btree.lookup(-1);
        assertFalse(it.hasNext());

        removeKey(50);
        it = btree.lookup(50);
        assertFalse(it.hasNext());
    }

    @Test
    public void testDuplicateKeysSkipInLookup() {
        assumeTrue(indexType == SORTED);
        Integer indexKey = 10;

        // Insert duplicate keys to span multiple B+tree leaf nodes
        for (int i = 0; i < 100; ++i) {
            String mapKey = "Name_" + i;
            String value = "Value_" + i;
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            btree.insert(indexKey, mapKeyData, valueData);
        }

        insertKey(11);
        insertKey(12);
        assertEquals(102, queryKeysCount());
        Iterator<Map.Entry> it = btree.lookup(10, false, null, true);
        assertTrue(it.hasNext());
        assertEquals("Name_11", it.next().getKey());
        assertTrue(it.hasNext());
        assertEquals("Name_12", it.next().getKey());
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorResync() {
        assumeTrue(indexType == SORTED);
        assumeTrue(indexScanBatchSize == 0);
        // Fill in 9 leaf pages
        insertKeysCompact(9 * 9);

        // Position iterator on the left-most node
        Iterator<QueryableEntry> it = btree.lookup(2, true, null, true);
        assertTrue(it.hasNext());
        QueryableEntry entry = it.next();
        assertEquals("Value_2", entry.getValue());

        long leftChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 0);
        long leafChildSeqNum = getSequenceNumber(leftChildAddr);

        // Remove a few keys ahead
        assertNotNull(btree.remove(3, nativeData("Name_3")));
        assertEquals(++leafChildSeqNum, getSequenceNumber(leftChildAddr));
        assertNotNull(btree.remove(4, nativeData("Name_4")));
        assertEquals(++leafChildSeqNum, getSequenceNumber(leftChildAddr));

        assertEquals(7, getKeysCount(leftChildAddr));
        // Iterate through the rest of the keys on the node
        for (int i = 5; i < 9; ++i) {
            // Try next element in the iterator
            assertTrue(it.hasNext());
            assertEquals("Value_" + i, it.next().getValue());
        }

        long nextChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 1);

        allocatorCallback.clear();
        // Remove the next node ahead
        for (int i = 9; i < 18; ++i) {
            assertNotNull(btree.remove(i, nativeData("Name_" + i)));
        }
        assertEquals(nextChildAddr, maFreeAddr.get());

        // Iterator skips over the removed node
        assertTrue(it.hasNext());
        assertEquals("Value_" + 18, it.next().getValue());

        allocatorCallback.clear();
        nextChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 1);
        // Remove the node iterator is currently on
        for (int i = 18; i < 27; ++i) {
            assertNotNull(btree.remove(i, nativeData("Name_" + i)));
        }
        assertEquals(nextChildAddr, maFreeAddr.get());

        nextChildAddr = innerNodeAccessor.getValueAddr(rootAddr, 1);
        // Iterator skips over the removed node
        assertTrue(it.hasNext());
        assertEquals("Value_" + 27, it.next().getValue());

        // Iterate to the end of the node
        for (int i = 28; i < 35; ++i) {
            assertTrue(it.hasNext());
            assertEquals("Value_" + i, it.next().getValue());
        }

        // Insert new key into the node, causing its split and moving
        // already seen keys ahead
        allocatorCallback.clear();
        insertKey(30, 0);
        assertTrue(maAllocateAddr.hasUpdates());

        // iterator skips over moved ahead keys
        assertTrue(it.hasNext());
        assertEquals("Value_" + 35, it.next().getValue());

        // Remove everything ahead of the iterator
        for (int i = 35; i < 81; ++i) {
            assertNotNull(btree.remove(i, nativeData("Name_" + i)));
        }

        // Iterator has reached the end
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorResyncWithBatching() {
        assumeTrue(indexType == SORTED);
        assumeTrue(indexScanBatchSize > 0);
        // Fill in 9 leaf pages
        insertKeysCompact(7);

        // Position iterator on the left-most node
        Iterator<QueryableEntry> it = btree.lookup(2, true, null, true);
        assertTrue(it.hasNext());
        QueryableEntry entry = it.next();
        assertEquals("Value_2", entry.getValue());

        assertTrue(it.hasNext());
        entry = it.next();
        assertEquals("Value_3", entry.getValue());

        // Remove a few keys ahead
        assertNotNull(btree.remove(4, nativeData("Name_4")));
        assertNotNull(btree.remove(5, nativeData("Name_5")));

        // All result has been cached in the batch
        for (int i = 4; i < 7; ++i) {
            // Try next element in the iterator
            assertTrue(it.hasNext());
            assertEquals("Value_" + i, it.next().getValue());
        }

        // Iterator has reached the end
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorResyncOnLookup() {
        assumeTrue(indexScanBatchSize == 0);
        int uniqueKeysCount = 100;
        insertKeys(uniqueKeysCount);

        // Insert some duplicates
        int entryKeyIndex = 11;
        for (int i = 0; i < 10; ++i) {
            Integer indexKey = 10;
            String mapKey = "Name_" + entryKeyIndex;
            String value = "Value_" + ++entryKeyIndex;
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            btree.insert(indexKey, mapKeyData, valueData);
        }

        Iterator it = btree.lookup(10);
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;

            if (count == 3) {
                // Remove all entries and make sure the iterator doesn;t return anything
                for (int i = 10; i < 21; ++i) {
                    NativeMemoryData entryKeyData = toNativeData("Name_" + i);
                    assertNotNull(btree.remove(10, entryKeyData));
                }

                assertFalse(it.hasNext());
                break;
            }
        }
    }

    @Test
    public void testInsertVariousTypeKeys() {
        assumeTrue(indexType == HASH);
        Integer integerKey = Integer.valueOf(100);
        Long longKey = Long.valueOf(Long.MAX_VALUE);
        String stringKey = "200";
        insertKey(integerKey);
        insertKey(longKey);
        insertKey(stringKey);
        assertEquals(3, queryKeysCount());

        assertHasKey(integerKey);
        assertHasKey(integerKey);
        assertHasKey(stringKey);
    }

    @Test
    public void testGetKeys() {
        int uniqueKeysCount = 1000;
        insertKeys(uniqueKeysCount);

        // Insert some duplicates
        for (int i = 0; i < 500; ++i) {
            Integer indexKey = nextInt(uniqueKeysCount);
            String mapKey = "Name_" + nextInt(uniqueKeysCount);
            String value = "Value_" + nextInt(uniqueKeysCount);
            NativeMemoryData mapKeyData = ess.toData(mapKey, NATIVE);
            NativeMemoryData valueData = ess.toData(value, NATIVE);
            btree.insert(indexKey, mapKeyData, valueData);
        }

        assertEquals(uniqueKeysCount, queryUniqueIndexKeysCount());
        Set<Integer> indexKeys = new HashSet<>();
        Iterator<Data> it = btree.keys();
        while (it.hasNext()) {
            indexKeys.add(ess.toObject(it.next()));
        }

        assertEquals(uniqueKeysCount, indexKeys.size());

        for (int i = 0; i < uniqueKeysCount; ++i) {
            assertTrue(indexKeys.contains(i));
        }
    }

    private void assertIterator(Iterator<Map.Entry> it, int expectedCount, int startingEntryIndex) {
        int count = 0;
        int key = startingEntryIndex;
        while (it.hasNext()) {
            assertEquals("Name_" + (key++), it.next().getKey());
            ++count;
        }
        assertEquals(expectedCount, count);
    }

    private void assertIteratorCount(int expected, Comparable indexKey) {
        Iterator<Map.Entry> it = btree.lookup(indexKey);
        int count = 0;
        while (it.hasNext()) {
            Map.Entry entry = it.next();
            String mapKey = "Name_" + count;
            String value = "Value_" + count;
            assertEquals(mapKey, entry.getKey());
            assertEquals(value, entry.getValue());
            count++;
        }

        assertEquals(expected, count);
    }

    private void assertFromKeyIteratorCount(int expected, Comparable fromKey) {
        Iterator<Map.Entry> it = btree.lookup(fromKey, true, null, true);
        int count = 0;
        while (it.hasNext()) {
            Map.Entry entry = it.next();
            String mapKey = "Name_" + count;
            String value = "Value_" + count;
            assertEquals(mapKey, entry.getKey());
            assertEquals(value, entry.getValue());
            count++;
        }
        assertEquals(expected, count);
    }

    // for unit testing only
    Iterator<Map.Entry> allKeys() {
        return btree.lookup(null, true, null, true);
    }

    private void assertLeafSlotValues(long leafAddr, int expectedMinKey, int expectedMaxKey) {
        int keysCount = getKeysCount(leafAddr);
        assertEquals(expectedMaxKey - expectedMinKey + 1, keysCount);
        assertEquals(0, getNodeLevel(leafAddr));
        for (int i = 0; i < keysCount; ++i) {
            int indexKey = i + expectedMinKey;
            assertEquals(Integer.valueOf(indexKey), ess.toObject(leafNodeAccessor.getIndexKeyHeapData(leafAddr, i)));
            assertEquals("Name_" + indexKey, ess.toObject(leafNodeAccessor.getEntryKey(leafAddr, i)));
            assertEquals("Value_" + indexKey, ess.toObject(leafNodeAccessor.getValue(leafAddr, i)));
        }
    }

    void assertNestedKeysSorted(long nodeAddr, int expectedLowBound, int expectedHighBound, int expectedLevel, boolean hashOrdering) {
        int level = getNodeLevel(nodeAddr);
        assertTrue(level >= 0);
        assertEquals(expectedLevel, level);
        int keysCount = getKeysCount(nodeAddr);
        assertTrue(keysCount >= 0);
        if (level == 0) {
            assertKeysBoundaries(nodeAddr, expectedLowBound, expectedHighBound, leafNodeAccessor, hashOrdering);
        } else {
            assertKeysBoundaries(nodeAddr, expectedLowBound, expectedHighBound, innerNodeAccessor, hashOrdering);

            int prevBound = Integer.MIN_VALUE;
            for (int i = 0; i < keysCount; ++i) {
                long childAddr = innerNodeAccessor.getValueAddr(nodeAddr, i);
                assertNotEquals(NULL_ADDRESS, childAddr);
                assertNotNull(innerNodeAccessor.getIndexKeyHeapDataOrNull(nodeAddr, i));
                assertNotEquals(NULL_ADDRESS, innerNodeAccessor.getEntryKey(nodeAddr, i));

                int bound = ess.toObject(innerNodeAccessor.getIndexKeyHeapData(nodeAddr, i));
                assertNestedKeysSorted(childAddr, prevBound, bound, expectedLevel - 1, hashOrdering);
                prevBound = bound + 1;
            }
            // check the last pointer
            long childAddr = innerNodeAccessor.getValueAddr(nodeAddr, keysCount);
            assertNotEquals(NULL_ADDRESS, childAddr);
            assertNestedKeysSorted(childAddr, prevBound, Integer.MAX_VALUE, expectedLevel - 1, hashOrdering);
        }
    }

    private void assertKeysBoundaries(long nodeAddr, int expectedLowBound, int expectedHighBound, HDBTreeNodeBaseAccessor nodeAccesor,
                                      boolean hashOrdering) {
        if (hashOrdering) {
            assertKeysSortedByHash(nodeAddr, nodeAccesor);
        } else {
            assertKeysSorted(nodeAddr, nodeAccesor);
            int keysCount = getKeysCount(nodeAddr);
            int minIndexKey = ess.toObject(nodeAccesor.getIndexKeyHeapData(nodeAddr, 0));
            int maxIndexKey = ess.toObject(nodeAccesor.getIndexKeyHeapData(nodeAddr, keysCount - 1));
            assertTrue(expectedLowBound <= minIndexKey);
            assertTrue(expectedHighBound >= maxIndexKey);
        }
    }

    private void assertKeysCountAndSeqCount(long nodeAddr, int expectedKeysCount, long expectedSeqCount) {
        assertEquals(expectedKeysCount, getKeysCount(nodeAddr));
        assertEquals(expectedSeqCount, getSequenceNumber(nodeAddr));
    }

    private List<Long> getChildrenAddrs(long nodeAddr) {
        assertTrue(getNodeLevel(nodeAddr) >= 1);
        int keysCount = getKeysCount(nodeAddr);
        List<Long> addrs = new ArrayList<>(keysCount + 1);
        for (int i = 0; i <= keysCount; ++i) {
            addrs.add(innerNodeAccessor.getValueAddr(nodeAddr, i));
        }
        return addrs;
    }

}
