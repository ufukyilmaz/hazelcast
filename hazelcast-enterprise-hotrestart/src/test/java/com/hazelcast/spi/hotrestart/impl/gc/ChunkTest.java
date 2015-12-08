package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkTest {

    private StableValChunk stableValChunk;
    private KeyHandle keyHandle;
    private Record record;

    final long keyPrefix = 1;
    final long chunkseq = 7;
    final long youngestRecordSeq = 1000;
    final long chunkSize = 100;
    final long chunkGarbage = 90;
    final long recordSeq = 11;
    final int recordSize = 13;

    @Before public void setup() {
        final RecordMap records = new RecordMapOnHeap();
        keyHandle = new KeyOnHeap(keyPrefix, new byte[1]);
        records.putIfAbsent(keyPrefix, keyHandle, recordSeq, recordSize, false, 0);
        record = records.get(keyHandle);
        stableValChunk = new StableValChunk(
                chunkseq, records, 1, youngestRecordSeq, chunkSize, chunkGarbage, false, false);
    }

    @Test public void size_reportsCorrectly() {
        assertEquals(chunkSize, stableValChunk.size());
    }

    @Test public void retire_makesRecordDead() {
        stableValChunk.retire(keyHandle, record);
        assertFalse(record.isAlive());
    }

    @Test public void retire_updatesGarbage() {
        stableValChunk.retire(keyHandle, record);
        assertEquals(chunkGarbage + recordSize, stableValChunk.garbage);
    }

    @Test public void retire_decrementsLiveRecordCount() {
        stableValChunk.retire(keyHandle, record);
        assertEquals(0, stableValChunk.liveRecordCount);
    }

    @Test public void retire_incrementsGarbageCount() {
        stableValChunk.retire(keyHandle, record);
        assertEquals(1, record.garbageCount());
    }

    @Test public void retire__dontIncrementGarbageCount_doesntIncrementIt() {
        stableValChunk.retire(keyHandle, record, false);
        assertEquals(0, record.garbageCount());
    }

}
