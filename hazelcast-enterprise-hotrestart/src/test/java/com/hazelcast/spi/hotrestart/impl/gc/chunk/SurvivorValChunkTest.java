package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordDataHolder;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordOnHeap;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.test.HazelcastParallelClassRunner;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SurvivorValChunkTest {

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final int recordSeq = 11;
    private final long keyPrefix = 1;
    private final byte[] keyBytes = new byte[100];
    private final byte[] valueBytes = new byte[200];
    private final int recordSize = Record.VAL_HEADER_SIZE + keyBytes.length + valueBytes.length;
    private final KeyHandle keyHandle = new KeyOnHeap(keyPrefix, keyBytes);
    private final Record record = new RecordOnHeap(recordSeq, recordSize, false, 0);
    private final RecordMap records = new RecordMapOnHeap();
    private final RecordDataHolder holder = new RecordDataHolder();

    private SurvivorValChunk survivorValChunk;
    private ChunkFileOut out;

    @Before
    public void before() {
        GcHelper gcHelper = mock(GcHelper.class);
        out = mock(ChunkFileOut.class);
        survivorValChunk = new SurvivorValChunk(7L, records, out, gcHelper);
        holder.getKeyBuffer(keyBytes.length).put(keyBytes);
        holder.getValueBuffer(valueBytes.length).put(valueBytes);
    }

    @Test
    public void when_add_then_writeToFile() {
        // When
        survivorValChunk.add(record, keyHandle, holder);

        // Then
        Mockito.verify(out, times(1)).writeValueRecord(record, keyPrefix, holder.getKeyBuffer(0), holder.getValueBuffer(0));
    }

    @Test
    public void when_add_then_sizeIncreases() {
        // When
        survivorValChunk.add(record, keyHandle, holder);

        // Then
        assertEquals(recordSize, survivorValChunk.size());
    }

    @Test
    public void when_add_then_updateMetadata() {
        // When
        survivorValChunk.add(record, keyHandle, holder);

        // Then
        final Record actual = records.get(keyHandle);
        assertEquals(record.rawSizeValue(), actual.rawSizeValue());
        assertEquals(record.additionalInt(), actual.additionalInt());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void insertOrUpdate_unsupported() {
        survivorValChunk.insertOrUpdate(0, 0, null, 0, 0);
    }

    @Test
    public void toStableChunk_returnsStableChunkOfSameSize() {
        // Given
        survivorValChunk.add(record, keyHandle, holder);

        // When
        final StableValChunk stableChunk = survivorValChunk.toStableChunk();

        // Then
        assertEquals(stableChunk.size(), survivorValChunk.size());
    }
}
