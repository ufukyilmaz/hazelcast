package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.impl.gc.AbstractOnHeapOffHeapTest;
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

import static com.hazelcast.internal.hotrestart.impl.HotRestarter.BUFFER_SIZE;
import static com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOffHeap.newRecordMapOffHeap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordTest extends AbstractOnHeapOffHeapTest {

    private final int seq = 3;
    private final int size = 57;
    private final int garbageCount = 5;

    private final int tombstoneSeq = 13;
    private final int tombstoneSize = 14;
    private final int filePosition = 77;

    private Record record;
    private Record tombstone;

    private RecordMapOffHeap containerMap;

    @Before
    public void setup() {
        if (offHeap) {
            containerMap = newRecordMapOffHeap(memMgr, memMgr);
            containerMap.putIfAbsent(keyPrefix, keyHandle, seq, size, false, garbageCount);
            containerMap.putIfAbsent(tombstoneKeyPrefix, tombstoneKeyHandle,
                    tombstoneSeq, tombstoneSize, true, filePosition);
        } else {
            record = new RecordOnHeap(seq, size, false, garbageCount);
            tombstone = new RecordOnHeap(tombstoneSeq, tombstoneSize, true, filePosition);
        }
    }

    @After
    @Override
    public void destroy() {
        if (containerMap != null) {
            containerMap.dispose();
        }
    }

    @Test
    public void payloadSize_returnsSizeMinusHeaderSize() {
        assertEquals(size - Record.VAL_HEADER_SIZE, record().payloadSize());
    }

    @Test
    public void filePosition_reportsCorrectly() {
        assertEquals(filePosition, tombstone().filePosition());
    }

    @Test
    public void when_setFilePosition_thenfilePosition_reportsCorrectly() {
        // When
        final int newFilePos = 13;
        tombstone().setFilePosition(newFilePos);

        // Then
        assertEquals(newFilePos, tombstone().filePosition());
    }

    @Test
    public void toString_returnsSomeString() {
        assertNotNull(record().toString());
        assertNotNull(tombstone().toString());
    }

    @Test
    public void staticSizeMethod_reportsCorrectly() {
        // Given
        final byte[] key = {1, 2};
        final byte[] value = {3, 5, 7};

        // When - Then
        assertEquals(Record.VAL_HEADER_SIZE + key.length + value.length, Record.size(key, value));
        assertEquals(Record.TOMB_HEADER_SIZE + key.length, Record.size(key, null));
    }

    @Test
    public void positionInUnitsOfBufSize_reportsCorrectly() {
        assertEquals(1, Record.positionInUnitsOfBufsize(BUFFER_SIZE));
    }

    @Test
    public void deadOrAliveSeq_onLiveRecord_reportsCorrectSeq() {
        assertEquals(seq, record().deadOrAliveSeq());
    }

    @Test
    public void deadOrAliveSeq_onDeadRecord_reportsCorrectSeq() {
        record().negateSeq();
        assertEquals(seq, record().deadOrAliveSeq());
    }

    @Test
    public void liveSeq_onLiveRecord_reportsCorrectSeq() {
        assertEquals(seq, record().liveSeq());
    }

    @Test
    public void deadSeq_onDeadRecord_reportsCorrectSeq() {
        record().negateSeq();
        assertEquals(seq, record().deadSeq());
    }

    @Test
    public void isAlive_worksForLiveAndDeadOnes() {
        assertTrue(record().isAlive());
        record().negateSeq();
        assertFalse(record().isAlive());
    }

    @Test
    public void rawSeqValue_onDeadRecord_reportsNegativeSeq() {
        record().negateSeq();
        assertEquals(-seq, record().rawSeqValue());
    }

    @Test
    public void size_onNormalRecord_reportsCorrectSize() {
        assertEquals(size, record().size());
    }

    @Test
    public void size_onTombstone_reportsCorrectSize() {
        assertEquals(tombstoneSize, tombstone().size());
    }

    @Test
    public void isTombstone_worksForBothTypes() {
        assertFalse(record().isTombstone());
        assertTrue(tombstone().isTombstone());
    }

    @Test
    public void garbageCount_reportsCorrectCount() {
        assertEquals(garbageCount, record().garbageCount());
    }

    @Test
    public void update_seq_updatesIt() {
        record().update(seq + 1, size);
        assertEquals(seq + 1, record().liveSeq());
    }

    @Test
    public void update_size_updatesIt() {
        record().update(seq, size + 1);
        assertEquals(size + 1, record().size());
        assertFalse(record().isTombstone());
    }

    @Test
    public void retireRecord_makesItDead() {
        record().retire(true);
        assertEquals(seq, record().deadSeq());
    }

    @Test
    public void retireRecord_keepsSize() {
        record().retire(true);
        assertEquals(size, record().size());
    }

    @Test
    public void retireRecord_incrementingGarbage_incrementsIt() {
        record().retire(true);
        assertEquals(garbageCount + 1, record().garbageCount());
    }

    @Test
    public void retireRecord_notIncrementingGarbage_doesntIncrementGarbage() {
        final Record r = record();
        r.retire(false);
        assertEquals(garbageCount, r.garbageCount());
    }

    @Test
    public void keyPrefix_reportsCorrectValue() {
        assertEquals(keyPrefix, record().keyPrefix(keyHandle));
    }

    @Test
    public void rawSeqValue_afterNegateSeq_reportsCorrectValue() {
        record().negateSeq();
        tombstone().negateSeq();
        assertEquals(-seq, record().rawSeqValue());
        assertEquals(-tombstoneSeq, tombstone().rawSeqValue());
    }

    @Test
    public void rawiSizeValue_reportsCorrectValue() {
        assertEquals(size, record().rawSizeValue());
        assertEquals(-tombstoneSize, tombstone().rawSizeValue());
    }

    @Test
    public void decrementGarbageCount_decrementsIt() {
        record().decrementGarbageCount();
        assertEquals(garbageCount - 1, record().garbageCount());
    }

    @Test
    public void incrementGarbageCount_incrementsIt() {
        record().incrementGarbageCount();
        assertEquals(garbageCount + 1, record().garbageCount());
    }

    @Test
    public void setGarbageCount_setsIt() {
        final int newCount = 100;
        record().setGarbageCount(newCount);
        assertEquals(newCount, record().garbageCount());
    }

    @Test
    public void setRawSeqSize_setsThem() {
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
