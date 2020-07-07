package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getLockState;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getSequenceNumber;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.setKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.setNodeLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeInnerAccessorTest extends BPlusTreeTestSupport {
    private static final int NODE_SIZE = 256;

    private HDBTreeInnerNodeAccessor innerAccessor;
    private HDBTreeLeafNodeAccessor leafAccessor;

    @Before
    public void setUp() {
        super.setUp();
        NodeSplitStrategy nodeSplitStrategy = new DefaultNodeSplitStrategy();
        innerAccessor = new HDBTreeInnerNodeAccessor(MOCKED_LOCK_MANAGER, ess, new DefaultBPlusTreeKeyComparator(ess),
                new DefaultBPlusTreeKeyAccessor(ess), keyAllocator,
                delegatingIndexAllocator, NODE_SIZE, nodeSplitStrategy);
        leafAccessor = new HDBTreeLeafNodeAccessor(MOCKED_LOCK_MANAGER, ess, new DefaultBPlusTreeKeyComparator(ess),
                new DefaultBPlusTreeKeyAccessor(ess), keyAllocator,
                delegatingIndexAllocator, NODE_SIZE, nodeSplitStrategy);
    }

    @Test
    public void testNewNode() {
        long nodeAddr = innerAccessor.newNodeLocked(newLockingContext());
        assertEquals(0, getLockState(nodeAddr));
        assertEquals(0, getNodeLevel(nodeAddr));
        assertEquals(0, getKeysCount(nodeAddr));
        assertEquals(0, getSequenceNumber(nodeAddr));
        assertEquals(0, getNodeLevel(nodeAddr));
    }

    @Test
    public void testCreateRoot() {
        long rootAddr = innerAccessor.newNodeLocked(newLockingContext());
        long newChildAddr = leafAccessor.newNodeLocked(newLockingContext());

        setNodeLevel(rootAddr, 1);
        setKeysCount(rootAddr, 0);
        innerAccessor.insert(rootAddr, newChildAddr);
        assertEquals(1, getSequenceNumber(rootAddr));
        assertEquals(0, getKeysCount(rootAddr));
        assertEquals(0, innerAccessor.lowerBound(rootAddr, Integer.valueOf(5), null));
        assertEquals(0, innerAccessor.lowerBound(rootAddr, Integer.valueOf(100), null));

        assertEquals(newChildAddr, innerAccessor.getValueAddr(rootAddr, 0));
    }

    @Test
    public void testInsertKeysAndLowerBound() {
        long rootAddr = innerAccessor.newNodeLocked(newLockingContext());
        long newChildAddr = leafAccessor.newNodeLocked(newLockingContext());

        setNodeLevel(rootAddr, 1);
        setKeysCount(rootAddr, 0);
        innerAccessor.insert(rootAddr, newChildAddr);

        int[] keys = randomKeys(8);

        for (int i = 0; i < 8; ++i) {
            NativeMemoryData indexKeyData = nativeData(keys[i]);
            NativeMemoryData mapKeyData = nativeData(200);
            newChildAddr = leafAccessor.newNodeLocked(newLockingContext());
            assertFalse(innerAccessor.isNodeFull(rootAddr));
            innerAccessor.insert(rootAddr, indexKeyData.address(), mapKeyData.address(), newChildAddr);
            assertEquals(i + 1, getKeysCount(rootAddr));
        }

        assertEquals(8, getKeysCount(rootAddr));
        assertEquals(9, getSequenceNumber(rootAddr));
        assertTrue(innerAccessor.isNodeFull(rootAddr));

        // assert the keys are sorted
        assertKeysSorted(rootAddr, 8);

        // assert lowerBound
        Arrays.sort(keys);
        int maxKey = keys[keys.length - 1];
        for (int i = 0; i < 8; ++i) {
            assertEquals(i, innerAccessor.lowerBound(rootAddr, keys[i], null));
        }
        assertEquals(0, innerAccessor.lowerBound(rootAddr, -1, null));
        assertEquals(8, innerAccessor.lowerBound(rootAddr, maxKey + 1, null));
    }

    @Test
    public void testInnerSplitDefaultStrategy() {
        testInnerSplit(new DefaultNodeSplitStrategy());
    }

    @Test
    public void testInnerSplitNewNodeEmptyStrategy() {
        testInnerSplit(new EmptyNewNodeSplitStrategy());
    }

    private void testInnerSplit(NodeSplitStrategy nodeSplitStrategy) {
        innerAccessor.setNodeSplitStrategy(nodeSplitStrategy);
        leafAccessor.setNodeSplitStrategy(nodeSplitStrategy);
        long rootAddr = innerAccessor.newNodeLocked(newLockingContext());
        long newChildAddr = leafAccessor.newNodeLocked(newLockingContext());

        setNodeLevel(rootAddr, 1);
        setKeysCount(rootAddr, 0);
        innerAccessor.insert(rootAddr, newChildAddr);

        for (int i = 0; i < 8; ++i) {
            NativeMemoryData indexKeyData = nativeData(i);
            NativeMemoryData mapKeyData = nativeData(200);
            newChildAddr = leafAccessor.newNodeLocked(newLockingContext());
            innerAccessor.insert(rootAddr, indexKeyData.address(), mapKeyData.address(), newChildAddr);
        }

        assertEquals(8, innerAccessor.lowerBound(rootAddr, 10, null));
        assertEquals(9, getSequenceNumber(rootAddr));
        assertTrue(innerAccessor.isNodeFull(rootAddr));
        assertEquals(8, getKeysCount(rootAddr));

        long newInnerAddr = innerAccessor.split(rootAddr, newLockingContext());
        int expectedNewInnerNodeKeysCount = nodeSplitStrategy.getNewNodeKeysCount(8);
        int expectedRootNodeExpectedKeys = expectedNewInnerNodeKeysCount == 0 ? 7 : 3;

        assertEquals(expectedRootNodeExpectedKeys, getKeysCount(rootAddr));
        assertEquals(expectedNewInnerNodeKeysCount, getKeysCount(newInnerAddr));

        assertKeysSorted(rootAddr, expectedRootNodeExpectedKeys);
        assertKeysSorted(newInnerAddr, expectedNewInnerNodeKeysCount);
        assertEquals(1, getSequenceNumber(newInnerAddr));

        assertEquals(expectedRootNodeExpectedKeys, innerAccessor.lowerBound(rootAddr, 10, null));
        assertEquals(expectedNewInnerNodeKeysCount, innerAccessor.lowerBound(newInnerAddr, 10, null));
    }


    private void assertKeysSorted(long nodeAddr, int count) {
        Integer prevKey = null;
        for (int i = 0; i < count; ++i) {
            Integer key = ess.toObject(innerAccessor.getIndexKeyHeapData(nodeAddr, i));
            if (prevKey != null) {
                assertTrue(prevKey <= key);
            }
            prevKey = key;
        }
    }

    private int[] randomKeys(int count) {
        int[] list = new int[count];
        Random r = new Random();
        for (int i = 0; i < count; ++i) {
            list[i] = r.nextInt(10000);
        }
        return list;
    }
}
