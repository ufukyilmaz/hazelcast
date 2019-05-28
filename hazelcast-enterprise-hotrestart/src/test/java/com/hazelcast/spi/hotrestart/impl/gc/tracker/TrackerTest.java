package com.hazelcast.spi.hotrestart.impl.gc.tracker;

import com.hazelcast.spi.hotrestart.impl.gc.AbstractOnHeapOffHeapTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrackerTest extends AbstractOnHeapOffHeapTest {

    private final int chunkSeq = 3;
    private final int tombstoneChunkSeq = 13;

    private TrackerMapBase containerMap;

    @Before
    public void setup() {
        containerMap = offHeap ? new TrackerMapOffHeap(memMgr, null) : new TrackerMapOnHeap();
        containerMap.putIfAbsent(keyHandle, chunkSeq, false);
        containerMap.putIfAbsent(tombstoneKeyHandle, tombstoneChunkSeq, true);
    }

    @After
    @Override
    public void destroy() {
        if (containerMap != null) {
            containerMap.dispose();
        }
        super.destroy();
    }

    @Test
    public void setThenGetGarbageCount_consistent() {
        final int count = 18;
        tracker().setGarbageCount(count);
        assertEquals(count, tracker().garbageCount());
    }

    @Test
    public void setThenGetRawChunkSeq_consistent() {
        final int chunkSeq = 18;
        tracker().setRawChunkSeq(chunkSeq);
        assertEquals(chunkSeq, tracker().rawChunkSeq());
    }

    @Test
    public void isAlive_reportsTrue_andAfterRetire_reportsFalse() {
        assertTrue(tracker().isAlive());
        tracker().retire(containerMap);
        assertFalse(tracker().isAlive());
    }

    @Test
    public void chunkSeq_reportsCorrectValue() {
        assertEquals(chunkSeq, tracker().chunkSeq());
        assertEquals(tombstoneChunkSeq, tombstoneTracker().chunkSeq());
    }

    @Test
    public void isTombstone_reportsCorrectValue() {
        assertFalse(tracker().isTombstone());
        assertTrue(tombstoneTracker().isTombstone());
    }

    @Test
    public void moveToChunk_updatesChunkSeq_andDoesntDisturbIsTombstone() {
        final int newChunkSeq = 1001;
        tracker().moveToChunk(newChunkSeq);
        assertEquals(newChunkSeq, tracker().chunkSeq());
        assertFalse(tracker().isTombstone());

        tombstoneTracker().moveToChunk(newChunkSeq);
        assertEquals(newChunkSeq, tombstoneTracker().chunkSeq());
        assertTrue(tombstoneTracker().isTombstone());
    }

    @Test
    public void newLiveTombstone_onLiveTracker_incrementsGarbageCount_andSetsNewState() {
        final int newChunkSeq = 1001;
        tracker().newLiveRecord(newChunkSeq, true, containerMap, false);
        assertEquals(1, tracker().garbageCount());
        assertEquals(newChunkSeq, tracker().chunkSeq());
        assertTrue(tracker().isTombstone());
    }

    @Test
    public void newLiveRecord_onLiveTombstoneTracker_leavesGarbageCount_andSetsNewState() {
        final int newChunkSeq = 1001;
        tombstoneTracker().newLiveRecord(newChunkSeq, false, containerMap, false);
        assertEquals(0, tombstoneTracker().garbageCount());
        assertEquals(newChunkSeq, tombstoneTracker().chunkSeq());
        assertFalse(tombstoneTracker().isTombstone());
    }

    @Test
    public void incrementGarbageCount_incrementsIt_andDecrement_decrementsIt() {
        tracker().incrementGarbageCount();
        assertEquals(1, tracker().garbageCount());
        tracker().reduceGarbageCount(1);
        assertEquals(0, tracker().garbageCount());
    }

    @Test
    public void resetGarbageCount_zeroesIt() {
        tracker().incrementGarbageCount();
        assertEquals(1, tracker().garbageCount());
        tracker().resetGarbageCount();
        assertEquals(0, tracker().garbageCount());
    }

    @Test
    public void setState_setsTheState() {
        final int newChunkSeq = 1001;
        tracker().setLiveState(newChunkSeq, true);
        assertEquals(newChunkSeq, tracker().chunkSeq());
        assertTrue(tracker().isTombstone());
    }

    private Tracker tracker() {
        return containerMap.get(keyHandle);
    }

    private Tracker tombstoneTracker() {
        return containerMap.get(tombstoneKeyHandle);
    }
}
