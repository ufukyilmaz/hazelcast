package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.ACTIVE_FNAME_SUFFIX;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.FNAME_SUFFIX;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ActiveValChunkTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final long keyPrefix = 1;
    private final long recordSeq = 11;
    private final byte[] keyBytes = new byte[100];
    private final byte[] valueBytes = new byte[200];
    private final KeyHandle keyHandle = new KeyOnHeap(keyPrefix, keyBytes);
    private final RecordMap records = new RecordMapOnHeap();

    private ActiveValChunk activeValChunk;
    private ChunkFileOut out;
    private GcHelper gcHelper;

    @Before
    public void before() {
        out = mock(ChunkFileOut.class);
        gcHelper = mock(GcHelper.class);
        activeValChunk = new ActiveValChunk(7L, records, out, gcHelper);
    }

    @Test
    public void when_addStep1_then_writeToFile() {
        // When
        activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes);

        // Then
        Mockito.verify(out, times(1)).writeValueRecord(recordSeq, keyPrefix, keyBytes, valueBytes);
    }

    @Test
    public void when_addStep1_then_sizeIncreases() {
        // When
        activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes);

        // Then
        assertEquals(Record.VAL_HEADER_SIZE + keyBytes.length + valueBytes.length, activeValChunk.size());
    }

    @Test
    @RequireAssertEnabled
    public void addStep1_whenSizeLimitReached() {
        // Given
        final int recordSize = Record.VAL_HEADER_SIZE + keyBytes.length + valueBytes.length;
        final int chunkSizeLimit = valChunkSizeLimit();

        // When-Then
        for (int size = recordSize; size < chunkSizeLimit; size += recordSize) {
            assertFalse(activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes));
        }
        assertTrue(activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes));
        exceptionRule.expect(AssertionError.class);
        activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes);
    }

    @Test
    public void addStep2_collectorVariant_updatesMetadata() {
        activeValChunk.addStep2(recordSeq, keyPrefix, keyHandle, 13);
        assertNotNull(records.get(keyHandle));
    }

    @Test
    public void addStep2_rebuilderVariant_updatesMetadata() {
        activeValChunk.addStep2(recordSeq, keyPrefix, keyHandle, 0, 13);
        assertNotNull(records.get(keyHandle));
    }

    @Test
    public void toStableChunk_returns_stableValChunkOfSameSize() {
        // Given
        activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes);

        // When
        final StableValChunk stable = activeValChunk.toStableChunk();

        // Then
        assertEquals(activeValChunk.size(), stable.size());
    }

    @Test
    public void insertOrUpdate_insertsValue() {
        final int recordSize = 13;

        // When
        activeValChunk.insertOrUpdate(recordSeq, keyPrefix, keyHandle, 0, recordSize);

        // Then
        final Record record = records.get(keyHandle);
        assertNotNull(record);
        assertFalse(record.isTombstone());
        assertEquals(recordSize, record.size());
    }

    @Test
    public void insertOrUpdateExisting_updatesValue() {
        final int recordSize = 13;

        // When
        activeValChunk.insertOrUpdate(recordSeq, keyPrefix, keyHandle, 0, recordSize);

        // Then
        final Record record = records.get(keyHandle);
        activeValChunk.insertOrUpdate(recordSeq, keyPrefix, keyHandle, 0, recordSize);
        assertSame(record, records.get(keyHandle));
    }

    @Test
    public void flagForFsync_propagatesToChunkFileOut() {
        // When
        activeValChunk.flagForFsyncOnClose(true);

        // Then
        Mockito.verify(out, times(1)).flagForFsyncOnClose(true);
    }

    @Test
    public void fsync_propagatesToChunkFileOut() {
        // When
        activeValChunk.fsync();

        // Then
        Mockito.verify(out, times(1)).fsync();
    }

    @Test
    public void close_propagatesToChunkFileOut() {
        // When
        activeValChunk.close();

        // Then
        Mockito.verify(out, times(1)).close();
    }

    @Test
    public void close_changesFileSuffix() {
        // When
        activeValChunk.close();

        // Then
        Mockito.verify(gcHelper, times(1)).changeSuffix(
                activeValChunk.base(), activeValChunk.seq, FNAME_SUFFIX + ACTIVE_FNAME_SUFFIX, FNAME_SUFFIX);
    }
}
