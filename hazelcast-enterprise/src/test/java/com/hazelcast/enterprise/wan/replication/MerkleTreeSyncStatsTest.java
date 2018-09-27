package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MerkleTreeSyncStatsTest {

    @Test
    public void testStats() {
        MerkleTreeSyncStats stats = new MerkleTreeSyncStats();

        // first partition
        stats.onSyncPartition();
        stats.onSyncLeaf(3);
        stats.onSyncLeaf(2);
        stats.onSyncLeaf(7);
        stats.onSyncLeaf(11);

        // second partition
        stats.onSyncPartition();
        stats.onSyncLeaf(5);
        stats.onSyncLeaf(9);
        stats.onSyncLeaf(7);

        assertEquals(2, stats.getPartitionsSynced());
        assertEquals(7, stats.getNodesSynced());
        assertEquals(2, stats.getMinLeafEntryCount());
        assertEquals(11, stats.getMaxLeafEntryCount());
        assertEquals(6.29D, stats.getAvgEntriesPerLeaf(), 10E-2);
        assertEquals(2.96D, stats.getStdDevEntriesPerLeaf(), 10E-2);
    }

    @Test
    public void testNoDeviance() {
        MerkleTreeSyncStats stats = new MerkleTreeSyncStats();

        // first partition
        stats.onSyncPartition();
        stats.onSyncLeaf(2);

        // second partition
        stats.onSyncPartition();
        stats.onSyncLeaf(2);

        assertEquals(2, stats.getPartitionsSynced());
        assertEquals(2, stats.getNodesSynced());
        assertEquals(2, stats.getMinLeafEntryCount());
        assertEquals(2, stats.getMaxLeafEntryCount());
        assertEquals(2.0D, stats.getAvgEntriesPerLeaf(), 10E-2);
        assertEquals(0.0D, stats.getStdDevEntriesPerLeaf(), 10E-2);
    }

    @Test
    public void testOneSyncedRecord() {
        MerkleTreeSyncStats stats = new MerkleTreeSyncStats();

        // first partition
        stats.onSyncPartition();
        stats.onSyncLeaf(1);

        assertEquals(1, stats.getPartitionsSynced());
        assertEquals(1, stats.getNodesSynced());
        assertEquals(1, stats.getMinLeafEntryCount());
        assertEquals(1, stats.getMaxLeafEntryCount());
        assertEquals(1.0D, stats.getAvgEntriesPerLeaf(), 10E-2);
        assertEquals(0.0D, stats.getStdDevEntriesPerLeaf(), 10E-2);
    }

    @Test
    public void testNoOverflow() {
        MerkleTreeSyncStats stats = new MerkleTreeSyncStats();

        // first partition
        stats.onSyncPartition();
        stats.onSyncLeaf(Integer.MAX_VALUE / 3);
        stats.onSyncLeaf(Integer.MAX_VALUE / 3 - 100);
        stats.onSyncLeaf(Integer.MAX_VALUE / 3 + 100);

        assertEquals(1, stats.getPartitionsSynced());
        assertEquals(3, stats.getNodesSynced());
        assertEquals(Integer.MAX_VALUE / 3 - 100, stats.getMinLeafEntryCount());
        assertEquals(Integer.MAX_VALUE / 3 + 100, stats.getMaxLeafEntryCount());
        assertEquals(Integer.MAX_VALUE / 3, stats.getAvgEntriesPerLeaf(), 10E-2);
        assertEquals(81.65D, stats.getStdDevEntriesPerLeaf(), 10E-2);
    }

    @Test
    public void testSyncDuration() {
        MerkleTreeSyncStats stats = new MerkleTreeSyncStats();

        HazelcastTestSupport.sleepSeconds(1);
        stats.onSyncComplete();

        assertTrue(stats.getSyncDurationSecs() >= 1);
    }
}
