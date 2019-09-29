package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.StableChunk.BY_BENEFIT_COST_DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StableValChunkTest {

    private StableValChunk stableValChunk;

    private final RecordMap records = new RecordMapOnHeap();
    private final long chunkSeq = 7;
    private final long chunkSize = 100;
    private final long chunkGarbage = 90;

    @Before
    public void setup() {
        final long keyPrefix = 1;
        final KeyHandle keyHandle = new KeyOnHeap(keyPrefix, new byte[1]);
        records.putIfAbsent(keyPrefix, keyHandle, 11L, 13, false, 0);
        stableValChunk = new StableValChunk(chunkSeq, records, 1, chunkSize, chunkGarbage, false);
    }

    @Test
    public void cost_returnsAmountOfLiveData() {
        assertEquals(chunkSize - chunkGarbage, stableValChunk.cost());
    }

    @Test
    public void benefit2costIsZeroOnNewestChunk() {
        assertEquals(0, stableValChunk.updateBenefitToCost(chunkSeq), 0);
    }

    @Test
    public void benefit2costIsInfiniteOnEmptyChunk() {
        // Given
        stableValChunk = new StableValChunk(chunkSeq, records, 1, 0, 0, false);
        final long currChunkSeq = chunkSeq + 10;

        // When - Then
        assertEquals(Double.POSITIVE_INFINITY, stableValChunk.updateBenefitToCost(currChunkSeq), 0);
    }

    @Test
    public void benefit2costIsZeroOnChunkWithNoGarbage() {
        // Given
        stableValChunk = new StableValChunk(chunkSeq, records, 1, chunkSize, 0, false);
        final long currChunkSeq = chunkSeq + 10;

        // When - Then
        assertEquals(0, stableValChunk.updateBenefitToCost(currChunkSeq), 0);
    }

    @Test
    public void benefit2costIsFiniteOnChunkWithSomeGarbage() {
        // Given
        stableValChunk = new StableValChunk(chunkSeq, records, 1, chunkSize, chunkGarbage, false);
        final long currChunkSeq = chunkSeq + 10;

        // When
        final double b2c = stableValChunk.updateBenefitToCost(currChunkSeq);

        // Then
        assertTrue(b2c > 0);
        assertTrue(b2c != Double.POSITIVE_INFINITY);
    }

    @Test
    public void cachedBenefit2costReturnsCachedValue() {
        // Given
        final long currChunkSeq = chunkSeq + 10;
        final double b2c = stableValChunk.updateBenefitToCost(currChunkSeq);

        // When - Then
        assertEquals(b2c, stableValChunk.cachedBenefitToCost(), 0.0);
    }

    @Test
    public void comparatorByb2c_comparesB2c() {
        // Given
        final double b2c_left = stableValChunk.updateBenefitToCost(chunkSeq + 10);
        final StableValChunk anotherChunk = new StableValChunk(chunkSeq, records, 1, chunkSize, chunkGarbage, false);
        final double b2c_right = anotherChunk.updateBenefitToCost(chunkSeq + 20);

        // When - Then
        assertEquals(Double.compare(b2c_right, b2c_left), BY_BENEFIT_COST_DESC.compare(stableValChunk, anotherChunk));
    }
}
