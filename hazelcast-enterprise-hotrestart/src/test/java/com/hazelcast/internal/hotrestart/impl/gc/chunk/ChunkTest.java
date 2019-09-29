package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordOnHeap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.FNAME_SUFFIX;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.TOMB_SIZE_LIMIT_DEFAULT;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.VAL_SIZE_LIMIT_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkTest {

    private StableValChunk chunk;
    private KeyHandle keyHandle;
    private Record record;

    private final RecordMap records = new RecordMapOnHeap();
    private final long chunkSeq = 7;
    private final long chunkSize = 100;
    private final long chunkGarbage = 90;
    private final long recordSeq = 11;
    private final int recordSize = 13;

    @Before
    public void setup() {
        final long keyPrefix = 1;
        keyHandle = new KeyOnHeap(keyPrefix, new byte[1]);
        records.putIfAbsent(keyPrefix, keyHandle, recordSeq, recordSize, false, 0);
        record = records.get(keyHandle);
        chunk = new StableValChunk(chunkSeq, records, 1, chunkSize, chunkGarbage, false);
    }

    @Test
    public void needsDismissing_setsTheFlag() {
        // When
        chunk.needsDismissing(true);

        // Then
        assertTrue(chunk.needsDismissing());
    }

    @Test
    public void size_reportsCorrectly() {
        assertEquals(chunkSize, chunk.size());
    }

    @Test
    public void retire_makesRecordDead() {
        // When
        chunk.retire(keyHandle, record);

        // Then
        assertFalse(record.isAlive());
    }

    @Test
    public void retire_updatesGarbage() {
        // When
        chunk.retire(keyHandle, record);

        // Then
        assertEquals(chunkGarbage + recordSize, chunk.garbage);
    }

    @Test
    public void retire_decrementsLiveRecordCount() {
        // When
        chunk.retire(keyHandle, record);

        // Then
        assertEquals(0, chunk.liveRecordCount);
    }

    @Test
    public void retire_incrementsGarbageCount() {
        // When
        chunk.retire(keyHandle, record);

        // Then
        assertEquals(1, record.garbageCount());
    }

    @Test
    public void retire_dontIncrementGarbageCount_doesntIncrementIt() {
        // When
        chunk.retire(keyHandle, record, false);

        // Then
        assertEquals(0, record.garbageCount());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void retire_withInconsistentKeyAndRecord_fails() {
        chunk.retire(keyHandle, new RecordOnHeap(recordSeq + 1, 20, false, 0));
    }

    @Test
    public void fnameSuffix() {
        assertEquals(FNAME_SUFFIX, chunk.fnameSuffix());
    }

    @Test
    public void base_reportsBasedirName() {
        assertEquals(VAL_BASEDIR, chunk.base());
    }

    @Test
    public void dispose_callsRecordsDispose() {
        final RecordMap records = Mockito.mock(RecordMap.class);
        new StableValChunk(chunkSeq, records, 1, chunkSize, chunkGarbage, false).dispose();
        Mockito.verify(records, times(1)).dispose();
    }

    @Test
    public void valChunkSizeLimit_returnsDefault() {
        assertEquals(VAL_SIZE_LIMIT_DEFAULT, Chunk.valChunkSizeLimit());
    }

    @Test
    public void tombChunkSizeLimit_returnsDefault() {
        assertEquals(TOMB_SIZE_LIMIT_DEFAULT, Chunk.tombChunkSizeLimit());
    }

    @Test
    public void toString_returnsSomething() {
        assertNotNull(chunk.toString());
    }
}
