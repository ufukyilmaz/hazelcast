package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getLockState;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getSequenceNumber;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeLeafAccessorTest extends BPlusTreeTestSupport {

    private static final int NODE_SIZE = 256;

    private HDBTreeLeafNodeAccessor leafAccessor;

    @Before
    public void setUp() {
        super.setUp();
        leafAccessor = new HDBTreeLeafNodeAccessor(MOCKED_LOCK_MANAGER, ess, new DefaultBPlusTreeKeyComparator(ess),
                new DefaultBPlusTreeKeyAccessor(ess, defaultAllocator), defaultAllocator, delegatingIndexAllocator,
                NODE_SIZE, new DefaultNodeSplitStrategy());
    }

    @Test
    public void testNewNode() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        assertEquals(0, getLockState(nodeAddr));
        assertEquals(0, getNodeLevel(nodeAddr));
        assertEquals(0, getKeysCount(nodeAddr));
        assertEquals(0, getSequenceNumber(nodeAddr));
        assertEquals(0, getNodeLevel(nodeAddr));
        assertEquals(NULL_ADDRESS, leafAccessor.getBackNode(nodeAddr));
        assertEquals(NULL_ADDRESS, leafAccessor.getForwNode(nodeAddr));
    }

    @Test
    public void testInsertKey() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        Integer indexKey = Integer.valueOf(1);
        NativeMemoryData indexKeyData = nativeData(indexKey);
        NativeMemoryData mapKeyData = nativeData(Integer.valueOf(200));
        NativeMemoryData valueData = nativeData("value");
        NativeMemoryData oldValueData = leafAccessor.insert(nodeAddr, indexKey, mapKeyData, valueData);
        assertNull(oldValueData);
        assertEquals(1, getKeysCount(nodeAddr));
        assertEquals(1, getSequenceNumber(nodeAddr));

        assertEquals(indexKeyData, leafAccessor.getIndexKeyHeapData(nodeAddr, 0));
        assertEquals(mapKeyData, leafAccessor.getEntryKey(nodeAddr, 0));
        assertEquals(valueData, leafAccessor.getValue(nodeAddr, 0));

        oldValueData = leafAccessor.insert(nodeAddr, indexKey, mapKeyData, nativeData("value2"));
        assertNotNull(oldValueData);
        assertEquals("value", ess.toObject(oldValueData));
    }

    @Test
    public void testInsertKeys() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        for (int i = 0; i < 9; ++i) {
            Integer indexKey = Integer.valueOf(i);
            NativeMemoryData mapKey = nativeData(Integer.valueOf(i + 200));
            NativeMemoryData value = nativeData("value_" + i);
            NativeMemoryData oldValue = leafAccessor.insert(nodeAddr, indexKey, mapKey, value);
            assertNull(oldValue);
        }
        assertEquals(9, getKeysCount(nodeAddr));
        assertEquals(9, getSequenceNumber(nodeAddr));
        assertTrue(leafAccessor.isNodeFull(nodeAddr));

        for (int i = 0; i < 9; ++i) {
            assertEquals(Integer.valueOf(i), ess.toObject(leafAccessor.getIndexKeyHeapData(nodeAddr, i)));
            assertEquals(Integer.valueOf(i + 200), ess.toObject(leafAccessor.getEntryKey(nodeAddr, i)));
            assertEquals("value_" + i, ess.toObject(leafAccessor.getValue(nodeAddr, i)));
        }
    }

    @Test
    public void testKeysSorted() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        Random r = new Random();

        // insert keys in random order
        for (int i = 0; i < 9; ++i) {
            int randomKey = r.nextInt();
            Integer indexKey = Integer.valueOf(randomKey);
            NativeMemoryData mapKey = nativeData(Integer.valueOf(randomKey + 200));
            NativeMemoryData value = nativeData("value_" + randomKey);
            NativeMemoryData oldValue = leafAccessor.insert(nodeAddr, indexKey, mapKey, value);
            assertNull(oldValue);
        }

        // assert the keys are sorted
        Integer prevKey = null;
        for (int i = 0; i < 9; ++i) {
            Integer key = ess.toObject(leafAccessor.getIndexKeyHeapData(nodeAddr, i));
            if (prevKey != null) {
                assertTrue(prevKey <= key);
            }
            prevKey = key;
        }
    }

    @Test
    public void testLowerBound() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        for (int i = 0; i < 9; ++i) {
            Integer indexKey = i * 2;
            NativeMemoryData mapKey = nativeData("mapKey_" + i);
            NativeMemoryData value = nativeData("value_" + i);
            NativeMemoryData oldValue = leafAccessor.insert(nodeAddr, indexKey, mapKey, value);
            assertNull(oldValue);
        }

        assertEquals(2, leafAccessor.lowerBound(nodeAddr, 4, null));
        assertEquals(3, leafAccessor.lowerBound(nodeAddr, 5, null));
        assertEquals(0, leafAccessor.lowerBound(nodeAddr, -1, null));
        assertEquals(9, leafAccessor.lowerBound(nodeAddr, 20, null));
    }

    @Test
    public void testRemoveKey() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        Integer indexKey = Integer.valueOf(1);
        NativeMemoryData mapKey = nativeData(Integer.valueOf(200));
        NativeMemoryData value = nativeData("value");
        leafAccessor.insert(nodeAddr, indexKey, mapKey, value);
        NativeMemoryData oldValue = leafAccessor.remove(nodeAddr, indexKey, mapKey);
        assertNotNull(oldValue);
        assertEquals("value", ess.toObject(oldValue));

        assertEquals(0, getKeysCount(nodeAddr));
        assertEquals(2, getSequenceNumber(nodeAddr));

        Integer notExistingKey = Integer.valueOf(2);
        oldValue = leafAccessor.remove(nodeAddr, notExistingKey, mapKey);
        assertNull(oldValue);

        assertEquals(0, getKeysCount(nodeAddr));
        assertEquals(2, getSequenceNumber(nodeAddr));
    }

    @Test
    public void testRemoveKeys() {
        long nodeAddr = prepareNode();

        assertEquals(9, getKeysCount(nodeAddr));
        assertEquals(9, getSequenceNumber(nodeAddr));

        // remove from the highest keys
        for (int i = 8; i >= 0; --i) {
            Integer indexKey = Integer.valueOf(i);
            NativeMemoryData mapKey = nativeData(Integer.valueOf(i + 200));
            NativeMemoryData oldValue = leafAccessor.remove(nodeAddr, indexKey, mapKey);
            assertNotNull(oldValue);
            assertEquals("value_" + i, ess.toObject(oldValue));
            int keysCount = getKeysCount(nodeAddr);
            assertEquals(i, keysCount);
            assertEquals(keysCount, leafAccessor.lowerBound(nodeAddr, indexKey, mapKey));
        }

        nodeAddr = prepareNode();

        // remove from the lowest keys
        for (int i = 0; i < 9; ++i) {
            Integer indexKey = Integer.valueOf(i);
            NativeMemoryData mapKey = nativeData(Integer.valueOf(i + 200));
            assertEquals(0, leafAccessor.lowerBound(nodeAddr, indexKey, mapKey));

            NativeMemoryData oldValue = leafAccessor.remove(nodeAddr, indexKey, mapKey);
            assertEquals(0, leafAccessor.lowerBound(nodeAddr, indexKey, mapKey));
            assertNotNull(oldValue);
            assertEquals("value_" + i, ess.toObject(oldValue));
            assertEquals(9 - i - 1, getKeysCount(nodeAddr));
        }

        nodeAddr = prepareNode();

        // remove key from the middle
        Integer indexKey = Integer.valueOf(5);
        NativeMemoryData mapKey = nativeData(Integer.valueOf(5 + 200));
        assertEquals(5, leafAccessor.lowerBound(nodeAddr, indexKey, mapKey));
        NativeMemoryData oldValue = leafAccessor.remove(nodeAddr, indexKey, mapKey);
        assertNotNull(oldValue);
        assertEquals("value_" + 5, ess.toObject(oldValue));
        assertEquals(5, leafAccessor.lowerBound(nodeAddr, indexKey, mapKey));
    }

    @Test
    public void testNodeSplit() {
        long nodeAddr = prepareNode();
        assertTrue(leafAccessor.isNodeFull(nodeAddr));

        long nodeSeqNumber = getSequenceNumber(nodeAddr);
        long newNodeAddr = leafAccessor.split(nodeAddr, newLockingContext());
        assertEquals(4, getKeysCount(nodeAddr));
        assertEquals(5, getKeysCount(newNodeAddr));
        assertEquals(nodeSeqNumber + 1, getSequenceNumber(nodeAddr));
        assertEquals(1, getSequenceNumber(newNodeAddr));

        assertFalse(leafAccessor.isNodeFull(nodeAddr));
        assertFalse(leafAccessor.isNodeFull(newNodeAddr));

        assertEquals(0, leafAccessor.lowerBound(nodeAddr, Integer.valueOf(0), null));
        assertEquals(4, leafAccessor.lowerBound(nodeAddr, Integer.valueOf(4), null));
        assertEquals(0, leafAccessor.lowerBound(newNodeAddr, Integer.valueOf(4), null));
        assertEquals(4, leafAccessor.lowerBound(newNodeAddr, Integer.valueOf(8), null));
        assertEquals(5, leafAccessor.lowerBound(newNodeAddr, Integer.valueOf(100), null));

        assertKeysSorted(nodeAddr);
        assertKeysSorted(newNodeAddr);
    }

    @Test
    public void testNodeSplitEmptyNewNodeStrategy() {
        leafAccessor.setNodeSplitStrategy(new EmptyNewNodeSplitStrategy());
        long nodeAddr = prepareNode();
        assertTrue(leafAccessor.isNodeFull(nodeAddr));

        long nodeSeqNumber = getSequenceNumber(nodeAddr);
        long newNodeAddr = leafAccessor.split(nodeAddr, new LockingContext());
        assertEquals(9, getKeysCount(nodeAddr));
        assertEquals(0, getKeysCount(newNodeAddr));
        assertEquals(nodeSeqNumber + 1, getSequenceNumber(nodeAddr));
        assertEquals(1, getSequenceNumber(newNodeAddr));

        assertTrue(leafAccessor.isNodeFull(nodeAddr));
        assertFalse(leafAccessor.isNodeFull(newNodeAddr));

        assertEquals(0, leafAccessor.lowerBound(nodeAddr, Integer.valueOf(0), null));
        assertEquals(8, leafAccessor.lowerBound(nodeAddr, Integer.valueOf(8), null));
        assertEquals(0, leafAccessor.lowerBound(newNodeAddr, Integer.valueOf(4), null));
        assertEquals(0, leafAccessor.lowerBound(newNodeAddr, Integer.valueOf(8), null));
        assertEquals(0, leafAccessor.lowerBound(newNodeAddr, Integer.valueOf(100), null));

        assertKeysSorted(nodeAddr);
    }

    void assertKeysSorted(long nodeAddr) {
        assertKeysSorted(nodeAddr, leafAccessor);
    }

    private long prepareNode() {
        long nodeAddr = leafAccessor.newNodeLocked(newLockingContext());
        for (int i = 0; i < 9; ++i) {
            Integer indexKey = Integer.valueOf(i);
            NativeMemoryData mapKey = nativeData(Integer.valueOf(i + 200));
            NativeMemoryData value = nativeData("value_" + i);
            leafAccessor.insert(nodeAddr, indexKey, mapKey, value);
        }
        return nodeAddr;
    }

}
