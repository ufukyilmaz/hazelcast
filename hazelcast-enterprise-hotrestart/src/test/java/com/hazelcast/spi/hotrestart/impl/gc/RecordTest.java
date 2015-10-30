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
public class RecordTest extends OnHeapOffHeapTestBase {

    private final int seq = 3;
    private final int size = 4;
    private final int garbageCount = 5;

    private final int tombstoneSeq = 13;
    private final int tombstoneSize = 14;
    private final int tombstoneGarbageCount = 15;

    private Record record;
    private Record tombstone;

    private RecordMapOffHeap containerMap;

    @Before public void setup() {
        if (offHeap) {
            containerMap = new RecordMapOffHeap(malloc);
            containerMap.putIfAbsent(keyPrefix, keyHandle, seq, size, false, garbageCount);
            containerMap.putIfAbsent(tombstoneKeyPrefix, tombstoneKeyHandle,
                    tombstoneSeq, tombstoneSize, true, tombstoneGarbageCount);
        } else {
            record = new RecordOnHeap(seq, size, false, garbageCount);
            tombstone = new RecordOnHeap(tombstoneSeq, tombstoneSize, true, tombstoneGarbageCount);
        }
    }

    @After public void destroy() {
        if (containerMap != null) {
            containerMap.dispose();
        }
    }

    @Test public void deadOrAliveSeq_onLiveRecord_reportsCorrectSeq() {
        assertEquals(seq, record().deadOrAliveSeq());
    }

    @Test public void deadOrAliveSeq_onDeadRecord_reportsCorrectSeq() {
        record().negateSeq();
        assertEquals(seq, record().deadOrAliveSeq());
    }

    @Test public void liveSeq_onLiveRecord_reportsCorrectSeq() {
        assertEquals(seq, record().liveSeq());
    }

    @Test public void deadSeq_onDeadRecord_reportsCorrectSeq() {
        record().negateSeq();
        assertEquals(seq, record().deadSeq());
    }

    @Test public void isAlive_worksForLiveAndDeadOnes() {
        assertTrue(record().isAlive());
        record().negateSeq();
        assertFalse(record().isAlive());
    }

    @Test public void rawSeqValue_onDeadRecord_reportsNegativeSeq() {
        record().negateSeq();
        assertEquals(-seq, record().rawSeqValue());
    }

    @Test public void size_onNormalRecord_reportsCorrectSize() {
        assertEquals(size, record().size());
    }

    @Test public void size_onTombstone_reportsCorrectSize() {
        assertEquals(tombstoneSize, tombstone().size());
    }

    @Test public void isTombstone_worksForBothTypes() {
        assertFalse(record().isTombstone());
        assertTrue(tombstone().isTombstone());
    }

    @Test public void garbageCount_reportsCorrectCount() {
        assertEquals(garbageCount, record().garbageCount());
        assertEquals(tombstoneGarbageCount, tombstone().garbageCount());
    }

    @Test public void update_seq_updatesIt() {
        record().update(seq + 1, size, true);
        assertEquals(seq + 1, record().liveSeq());
    }

    @Test public void update_size_updatesIt() {
        record().update(seq, size + 1, true);
        assertEquals(size + 1, record().size());
        assertEquals(true, record().isTombstone());
    }

    @Test public void update_isTombstone_updatesIt() {
        record().update(seq, size, true);
        assertEquals(true, record().isTombstone());
    }

    @Test public void retireRecord_makesItDead() {
        record().retire(true);
        assertEquals(seq, record().deadSeq());
    }

    @Test public void retireRecord_keepsSize() {
        record().retire(true);
        assertEquals(size, record().size());
    }

    @Test public void retireRecord_incrementingGarbage_incrementsIt() {
        record().retire(true);
        assertEquals(garbageCount + 1, record().garbageCount());
    }

    @Test public void retireRecord_notIncrementingGarbage_doesntIncrementGarbage() {
        final Record r = record();
        r.retire(false);
        assertEquals(garbageCount, r.garbageCount());
    }

    @Test public void retireTombstone_incrementingGarbage_doesntIncrementGarbage() {
        final Record r = tombstone();
        r.retire(true);
        assertEquals(tombstoneGarbageCount, r.garbageCount());
    }

    @Test public void keyPrefix_reportsCorrectValue() {
        assertEquals(keyPrefix, record().keyPrefix(keyHandle));
        assertEquals(tombstoneKeyPrefix, tombstone().keyPrefix(tombstoneKeyHandle));
    }

    @Test public void rawSeqValue_afterNegateSeq_reportsCorrectValue() {
        record().negateSeq();
        tombstone().negateSeq();
        assertEquals(-seq, record().rawSeqValue());
        assertEquals(-tombstoneSeq, tombstone().rawSeqValue());
    }

    @Test public void rawiSizeValue_reportsCorrectValue() {
        assertEquals(size, record().rawSizeValue());
        assertEquals(-tombstoneSize, tombstone().rawSizeValue());
    }

    @Test public void decrementGarbageCount_decrementsIt() {
        record().decrementGarbageCount();
        tombstone().decrementGarbageCount();
        assertEquals(garbageCount - 1, record().garbageCount());
        assertEquals(tombstoneGarbageCount - 1, tombstone().garbageCount());
    }

    @Test public void incrementGarbageCount_incrementsIt() {
        record().incrementGarbageCount();
        tombstone().incrementGarbageCount();
        assertEquals(garbageCount + 1, record().garbageCount());
        assertEquals(tombstoneGarbageCount + 1, tombstone().garbageCount());
    }

    @Test public void setGarbageCount_setsIt() {
        final int newCount = 100;
        record().setGarbageCount(newCount);
        tombstone().setGarbageCount(newCount);
        assertEquals(newCount, record().garbageCount());
        assertEquals(newCount, tombstone().garbageCount());
    }

    @Test public void setRawSeqSize_setsThem() {
        final int newSeq = 101;
        final int newSize = 1001;
        record().setRawSeqSize(newSeq, newSize);
        tombstone().setRawSeqSize(newSeq, -newSize);
        assertEquals(newSeq, record().liveSeq());
        assertEquals(newSeq, tombstone().liveSeq());
        assertEquals(newSize, record().size());
        assertEquals(newSize, tombstone().size());
    }

    private Record record() {
        return offHeap ? containerMap.get(keyHandle) : record;
    }

    private Record tombstone() {
        return offHeap ? containerMap.get(tombstoneKeyHandle) : tombstone;
    }
}
