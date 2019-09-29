package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.internal.hotrestart.impl.io.TombFileAccessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.tombChunkSizeLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteThroughTombChunkTest {

    public static final String FNAME_SUFFIX = ".testchunk";

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private WriteThroughTombChunk tombChunk;
    private ChunkFileOut out;

    private final long keyPrefix = 1;
    private final long recordSeq = 11;
    private final byte[] keyBytes = new byte[100];
    private final KeyHandle keyHandle = new KeyOnHeap(keyPrefix, keyBytes);
    private final RecordMap records = new RecordMapOnHeap();

    @Before
    public void before() {
        out = mock(ChunkFileOut.class);
        tombChunk = new WriteThroughTombChunk(7L, FNAME_SUFFIX, records, out, mock(GcHelper.class));
    }

    @Test
    public void base_returnsTombBase() {
        assertEquals(Chunk.TOMB_BASEDIR, tombChunk.base());
    }

    @Test
    public void needsDismissing_hasNoEffect() {
        // When
        tombChunk.needsDismissing(true);

        // Then
        assertFalse(tombChunk.needsDismissing());
    }

    @Test
    public void when_addStep1_then_writeToFile() {
        // When
        tombChunk.addStep1(recordSeq, keyPrefix, keyBytes, null);

        // Then
        Mockito.verify(out, times(1)).writeTombstone(recordSeq, keyPrefix, keyBytes);
    }

    @Test
    public void when_addStep1_then_sizeIncreases() {
        // When
        tombChunk.addStep1(recordSeq, keyPrefix, keyBytes, null);

        // Then
        assertEquals(Record.TOMB_HEADER_SIZE + keyBytes.length, tombChunk.size());
    }

    @Test
    public void when_addStep1FromTfa_then_sizeIncreases() throws Exception {
        // Given
        final TombFileAccessor tfa = mock(TombFileAccessor.class);
        final int recordSize = 13;
        Mockito.when(tfa.loadAndCopyTombstone(0, out)).thenReturn(recordSize);

        // When
        tombChunk.addStep1(tfa, 0);

        // Then
        assertEquals(recordSize, tombChunk.size());
    }

    @Test
    public void when_tfaFails_then_addStep1WrapsException() throws Exception {
        // Given
        final TombFileAccessor tfa = mock(TombFileAccessor.class);
        Mockito.when(tfa.loadAndCopyTombstone(0, out)).thenThrow(new IOException());

        // When - Then
        exceptionRule.expect(HotRestartException.class);
        tombChunk.addStep1(tfa, 0);
    }

    @Test
    @RequireAssertEnabled
    public void test_addStep1_whenSizeLimitReached() {
        // Given
        final int recordSize = Record.TOMB_HEADER_SIZE + keyBytes.length;
        final int chunkSizeLimit = tombChunkSizeLimit();

        // When-Then
        for (int size = recordSize; size < chunkSizeLimit; size += recordSize) {
            assertFalse(tombChunk.addStep1(recordSeq, keyPrefix, keyBytes, null));
        }
        assertTrue(tombChunk.addStep1(recordSeq, keyPrefix, keyBytes, null));
        exceptionRule.expect(AssertionError.class);
        tombChunk.addStep1(recordSeq, keyPrefix, keyBytes, null);
    }

    @Test
    public void toStableChunk_returns_stableTombChunkOfSameSize() {
        // Given
        tombChunk.addStep1(recordSeq, keyPrefix, keyBytes, null);

        // When
        final StableTombChunk stable = tombChunk.toStableChunk();

        // Then
        assertEquals(tombChunk.size(), stable.size());
    }

    @Test
    public void insertOrUpdate_insertsTombstone() {
        final int recordSize = 13;

        // When
        tombChunk.insertOrUpdate(recordSeq, keyPrefix, keyHandle, 13, recordSize);

        // Then
        final Record record = records.get(keyHandle);
        assertNotNull(record);
        assertTrue(record.isTombstone());
        assertEquals(recordSize, record.size());
    }

    @Test
    public void insertOrUpdateExisting_updatesValue() {
        final int recordSize = 13;

        // When
        tombChunk.insertOrUpdate(recordSeq, keyPrefix, keyHandle, 0, recordSize);

        // Then
        final Record record = records.get(keyHandle);
        tombChunk.insertOrUpdate(recordSeq, keyPrefix, keyHandle, 0, recordSize);
        assertSame(record, records.get(keyHandle));
    }
}
