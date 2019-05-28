package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.merkletree.MerkleTreeUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.enterprise.wan.replication.ComparisonState.ABORTED;
import static com.hazelcast.enterprise.wan.replication.ComparisonState.FINISHED;
import static com.hazelcast.enterprise.wan.replication.ComparisonState.IN_PROGRESS;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MerkleTreeComparisonProcessorTest {

    @Test
    public void testInitialValues() {
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());
    }

    @Test
    public void testComparisonFinishesIfLocalReturnsSameLevel() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = nodeValues(1, new int[]{1, 12321324, 2, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processRemoteNodeValues(remote);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processLocalNodeValues(local);
        assertTrue(processor.isComparisonFinished());
        assertEquals(local, processor.getDifference());
        assertEquals(FINISHED, processor.getComparisonState());
    }

    @Test
    public void testComparisonFinishesIfLocalReturnsNullPartition() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = nodeValues(1, null);
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processRemoteNodeValues(remote);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        Map<Integer, int[]> expectedDifference = nodeValues(1, new int[]{0, MerkleTreeUtil.sumHash(12321324, 23423424)});
        processor.processLocalNodeValues(local);
        assertTrue(processor.isComparisonFinished());
        assertEquals(expectedDifference.keySet(), processor.getDifference().keySet());
        assertArrayEquals(expectedDifference.get(1), processor.getDifference().get(1));
        assertEquals(FINISHED, processor.getComparisonState());
    }

    @Test
    public void testComparisonFinishesIfLocalReturnsEmpty() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = emptyMap();
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processRemoteNodeValues(remote);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processLocalNodeValues(local);
        assertTrue(processor.isComparisonFinished());
        assertEquals(local, processor.getDifference());
        assertEquals(FINISHED, processor.getComparisonState());
    }

    @Test
    public void testComparisonContinuesIfLocalReturnsDeeperLevel() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = nodeValues(1, new int[]{3, 12321324, 4, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processRemoteNodeValues(remote);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processLocalNodeValues(local);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());
    }

    @Test(expected = AssertionError.class)
    public void testComparisonThrowsIfProcessLocalIsInvokedInFinalState() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = Collections.emptyMap();
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processRemoteNodeValues(remote);
        processor.processLocalNodeValues(local);
        assertTrue(processor.isComparisonFinished());

        processor.processLocalNodeValues(local);
    }

    @Test
    public void testComparisonFinishesIfRemoteReturnsSameLevel() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = nodeValues(1, new int[]{1, 12321324, 2, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processLocalNodeValues(local);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processRemoteNodeValues(remote);
        assertTrue(processor.isComparisonFinished());
        assertEquals(remote, processor.getDifference());
        assertEquals(FINISHED, processor.getComparisonState());
    }

    @Test
    public void testComparisonFinishesIfRemoteReturnsNullPartition() {
        Map<Integer, int[]> remote = nodeValues(1, null);
        Map<Integer, int[]> local = nodeValues(1, new int[]{1, 12321324, 2, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processLocalNodeValues(local);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processRemoteNodeValues(remote);
        assertTrue(processor.isComparisonFinished());
        assertEquals(local, processor.getDifference());
        assertEquals(FINISHED, processor.getComparisonState());
    }

    @Test
    public void testComparisonFinishesIfRemoteReturnsEmpty() {
        Map<Integer, int[]> remote = emptyMap();
        Map<Integer, int[]> local = nodeValues(1, new int[]{1, 12321324, 2, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processLocalNodeValues(local);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processRemoteNodeValues(remote);
        assertTrue(processor.isComparisonFinished());
        assertEquals(remote, processor.getDifference());
        assertEquals(FINISHED, processor.getComparisonState());
    }

    @Test
    public void testComparisonContinuesIfRemoteReturnsDeeperLevel() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{3, 45645645, 4, 34543543});
        Map<Integer, int[]> local = nodeValues(1, new int[]{1, 12321324, 2, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processLocalNodeValues(local);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processRemoteNodeValues(remote);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());
    }

    @Test
    public void testComparisonAbortsIfRemoteReturnsNull() {
        Map<Integer, int[]> remote = null;
        Map<Integer, int[]> local = nodeValues(1, new int[]{1, 12321324, 2, 23423423});
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processLocalNodeValues(local);
        assertFalse(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(IN_PROGRESS, processor.getComparisonState());

        processor.processRemoteNodeValues(remote);
        assertTrue(processor.isComparisonFinished());
        assertNull(processor.getDifference());
        assertEquals(ABORTED, processor.getComparisonState());
    }

    @Test(expected = AssertionError.class)
    public void testComparisonThrowsIfProcessRemoteIsInvokedInFinalState() {
        Map<Integer, int[]> remote = nodeValues(1, new int[]{1, 12321324, 2, 23423424});
        Map<Integer, int[]> local = Collections.emptyMap();
        MerkleTreeComparisonProcessor processor = new MerkleTreeComparisonProcessor();

        processor.processRemoteNodeValues(remote);
        processor.processLocalNodeValues(local);
        assertTrue(processor.isComparisonFinished());

        processor.processRemoteNodeValues(remote);
    }

    private static Map<Integer, int[]> nodeValues(int partitionId, int[] nodeValues) {
        HashMap<Integer, int[]> map = new HashMap<Integer, int[]>();
        map.put(partitionId, nodeValues);
        return map;
    }

}
