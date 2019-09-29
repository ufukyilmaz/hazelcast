package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StableTombChunkTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private StableTombChunk stableTombChunk;
    private Record record;
    private KeyHandle keyHandle;

    @Before
    public void before() {
        final RecordMapOnHeap records = new RecordMapOnHeap();
        final long keyPrefix = 13;
        keyHandle = new KeyOnHeap(keyPrefix, new byte[]{1});
        // long keyPrefix, KeyHandle kh, long seq, int size, boolean isTombstone, int filePosition
        records.putIfAbsent(keyPrefix, keyHandle, 1L, 10, true, 10);
        record = records.get(keyHandle);
        // long seq, RecordMap records, int liveRecordCount, long size, long garbage
        stableTombChunk = new StableTombChunk(1L, records, 1, 10L, 1L);
    }

    @Test
    public void baseReturnsTombBase() {
        assertEquals(TOMB_BASEDIR, stableTombChunk.base());
    }

    @Test
    public void initFilePosToKeyHandle_containsTheLiveRecord() {
        // When
        final int[] filePositions = stableTombChunk.initFilePosToKeyHandle();

        // Then
        assertEquals(1, filePositions.length);
        assertNotNull(stableTombChunk.getLiveKeyHandle(filePositions[0]));
    }

    @Test
    public void retireDuringGc_removesFromFileposToKeyhandle() {
        // Given
        final int[] filePositions = stableTombChunk.initFilePosToKeyHandle();

        // When
        stableTombChunk.retire(keyHandle, record, false);

        // Then
        assertNull(stableTombChunk.getLiveKeyHandle(filePositions[0]));
    }

    @Test
    public void disposeFileposToKeyhandle_removesTheMap() {
        // Given
        final int[] filePositions = stableTombChunk.initFilePosToKeyHandle();

        // When
        stableTombChunk.disposeFilePosToKeyHandle();

        // Then
        exceptionRule.expect(NullPointerException.class);
        stableTombChunk.getLiveKeyHandle(filePositions[0]);
    }

    @Test
    public void when_updateBenefitToCost_then_cachedBenefitToCostReportsIt() {
        // Given
        final long size = 10L;
        final long garbage = 1L;
        stableTombChunk = new StableTombChunk(1L, null, 1, size, garbage);

        // When
        stableTombChunk.updateBenefitToCost();

        // Then
        assertEquals(StableTombChunk.benefitToCost(garbage, size), stableTombChunk.cachedBenefitToCost(), 0.0);
    }

    @Test
    public void benefitToCost_forEmptyChunk_isInfinite() {
        assertEquals(Double.POSITIVE_INFINITY, StableTombChunk.benefitToCost(0, 0), 0.0);
    }

    @Test
    public void benefitToCost_forAllGarbageChunk_isInfinite() {
        assertEquals(Double.POSITIVE_INFINITY, StableTombChunk.benefitToCost(1, 1), 0.0);
    }

    @Test
    public void benefitToCost_forChunkWithSomeGarbage_isFinite() {
        assertTrue(StableTombChunk.benefitToCost(1, 2) > 0);
    }
}
