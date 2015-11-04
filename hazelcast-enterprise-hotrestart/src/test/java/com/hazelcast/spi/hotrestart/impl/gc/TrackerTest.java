package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TrackerTest extends OnHeapOffHeapTestBase {

    private final int chunkSeq = 3;
    private final int tombstoneChunkSeq = 13;

    private Tracker tracker;
    private Tracker tombstoneTracker;

    private TrackerMapOffHeap containerMap;

    @Before public void setup() {
        if (offHeap) {
            containerMap = new TrackerMapOffHeap(malloc);
            containerMap.putIfAbsent(keyHandle, chunkSeq, false);
            containerMap.putIfAbsent(tombstoneKeyHandle, tombstoneChunkSeq, true);
        } else {
            tracker = new TrackerOnHeap(chunkSeq, false);
            tombstoneTracker = new TrackerOnHeap(tombstoneChunkSeq, true);
        }
    }

    @After public void destroy() {
        if (containerMap != null) {
            containerMap.dispose();
        }
    }

    @Test public void setThenGetGarbageCount_consistent() {
        final int count = 18;
        tracker().setGarbageCount(count);
        assertEquals(count, tracker().garbageCount());
    }

    @Test public void setThenGetRawChunkSeq_consistent() {
        final int chunkSeq = 18;
        tracker().setRawChunkSeq(chunkSeq);
        assertEquals(chunkSeq, tracker().rawChunkSeq());
    }

    @Test public void isAlive_reportsTrue_andAfterRetire_reportsFalse() {
        assertTrue(tracker().isAlive());
        tracker().retire();
        assertFalse(tracker().isAlive());
    }

    @Test public void chunkSeq_reportsCorrectValue() {
        assertEquals(chunkSeq, tracker().chunkSeq());
        assertEquals(tombstoneChunkSeq, tombstoneTracker().chunkSeq());
    }

    @Test public void isTombstone_reportsCorrectValue() {
        assertFalse(tracker().isTombstone());
        assertTrue(tombstoneTracker().isTombstone());
    }

    @Test public void moveToChunk_updatesChunkSeq_andDoesntDisturbIsTombstone() {
        final int newChunkSeq = 1001;
        tracker().moveToChunk(newChunkSeq);
        assertEquals(newChunkSeq, tracker().chunkSeq());
        assertFalse(tracker().isTombstone());

        tombstoneTracker().moveToChunk(newChunkSeq);
        assertEquals(newChunkSeq, tombstoneTracker().chunkSeq());
        assertTrue(tombstoneTracker().isTombstone());
    }

    @Test public void newLiveTombstone_onLiveTracker_incrementsGarbageCount_andSetsNewState() {
        final int newChunkSeq = 1001;
        tracker().newLiveRecord(newChunkSeq, true);
        assertEquals(1, tracker().garbageCount());
        assertEquals(newChunkSeq, tracker().chunkSeq());
        assertTrue(tracker().isTombstone());
    }

    @Test public void newLiveRecord_onLiveTombstoneTracker_leavesGarbageCount_andSetsNewState() {
        final int newChunkSeq = 1001;
        tombstoneTracker().newLiveRecord(newChunkSeq, false);
        assertEquals(0, tombstoneTracker().garbageCount());
        assertEquals(newChunkSeq, tombstoneTracker().chunkSeq());
        assertFalse(tombstoneTracker().isTombstone());
    }

    @Test public void incrementGarbageCount_incrementsIt_andDecrement_decrementsIt() {
        tracker().incrementGarbageCount();
        assertEquals(1, tracker().garbageCount());
        tracker().decrementGarbageCount(1);
        assertEquals(0, tracker().garbageCount());
    }

    @Test public void resetGarbageCount_zeroesIt() {
        tracker().incrementGarbageCount();
        assertEquals(1, tracker().garbageCount());
        tracker().resetGarbageCount();
        assertEquals(0, tracker().garbageCount());
    }

    @Test public void setState_setsTheState() {
        final int newChunkSeq = 1001;
        tracker().setState(newChunkSeq, true);
        assertEquals(newChunkSeq, tracker().chunkSeq());
        assertTrue(tracker().isTombstone());
    }

    private Tracker tracker() {
        return offHeap ? containerMap.get(keyHandle) : tracker;
    }

    private Tracker tombstoneTracker() {
        return offHeap ? containerMap.get(tombstoneKeyHandle) : tombstoneTracker;
    }
}
