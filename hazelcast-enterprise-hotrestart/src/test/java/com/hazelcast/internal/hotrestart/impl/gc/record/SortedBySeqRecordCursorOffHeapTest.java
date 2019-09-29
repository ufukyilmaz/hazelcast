package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.elastic.LongArray;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.hotrestart.KeyHandleOffHeap;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.test.AssertEnabledFilterRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.addrOfKey1At;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.addrOfKey2At;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.valueAddr2slotBase;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createMutatorCatchup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SortedBySeqRecordCursorOffHeapTest {

    private static final MemorySize MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    @Rule
    public final AssertEnabledFilterRule assertEnabledRule = new AssertEnabledFilterRule();

    private final MemoryManager memMgr = new MemoryManagerBean(new StandardMemoryManager(MEMORY_SIZE), AMEM);

    private MutatorCatchup mc;

    @Before
    public void before() {
        mc = createMutatorCatchup();
    }

    @Test(expected = NullPointerException.class)
    public void when_seqsAndSlotBasesAreNull_then_NPE() {
        new SortedBySeqRecordCursorOffHeap(null, 8, memMgr, mock(MutatorCatchup.class));
    }

    @Test
    public void advanceNonEmpty_returnsTrue() {
        LongArray seqsAndSlotBases = new LongArray(memMgr, 8);
        SortedBySeqRecordCursorOffHeap cursor = new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, 8, memMgr,
                mock(MutatorCatchup.class));

        assertTrue(cursor.advance());
    }

    @Test
    public void asKeyHandle_reflectsTheKeyHandleAtCursorPos() {
        // Given
        final long khAddress = 13;
        final long khSequence = 17;

        final long slotBase = memMgr.getAllocator().allocate(2 * LONG_SIZE_IN_BYTES);
        final MemoryAccessor mem = memMgr.getAccessor();
        mem.putLong(addrOfKey1At(slotBase), khAddress);
        mem.putLong(addrOfKey2At(slotBase), khSequence);
        final LongArray seqsAndSlotBases = new LongArray(memMgr, 2);
        seqsAndSlotBases.set(1, slotBase);
        final SortedBySeqRecordCursorOffHeap cursor = new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, 2, memMgr, mc);
        cursor.advance();

        // When
        final KeyHandleOffHeap kh = cursor.asKeyHandle();

        // Then
        assertEquals(khAddress, kh.address());
        assertEquals(khSequence, kh.sequenceId());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void when_advanceAfterDispose_then_assertionError() {
        // Given
        LongArray seqsAndSlotBases = new LongArray(memMgr, 8);
        SortedBySeqRecordCursorOffHeap cursor = new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, 8, memMgr, mc);
        cursor.dispose();

        // When - Then
        cursor.advance();
    }

    @Test
    public void advance_traversesInSequenceOrder() {
        // GIVEN
        int count = 1 << 11;
        LongArray seqsAndSlotBases = initSeqsAndSlotBases(memMgr, 1, count);

        // WHEN
        SortedBySeqRecordCursorOffHeap cursorOffHeap = new SortedBySeqRecordCursorOffHeap(
                seqsAndSlotBases, 2 * count, memMgr, mc);

        // THEN
        int actualCount = 0;
        for (int expected = 1; cursorOffHeap.advance(); expected++, actualCount++) {
            RecordOffHeap rec = ((RecordOffHeap) cursorOffHeap.asRecord());
            assertEquals("wrong record address", expected, rec.address);
        }
        assertEquals("wrong elements count in cursor", actualCount, count);
    }

    @Test
    public void mutatorCatchup_calledAtLeastCountTimes() {
        // GIVEN
        int count = 1 << 11;
        MutatorCatchup mc = mock(MutatorCatchup.class);
        LongArray seqsAndSlotBases = initSeqsAndSlotBases(memMgr, 1, count);

        // WHEN
        SortedBySeqRecordCursorOffHeap cursorOffHeap = new SortedBySeqRecordCursorOffHeap(
                seqsAndSlotBases, 2 * count, memMgr, mc);

        // THEN
        assertNotNull(cursorOffHeap);
        verify(mc, atLeast(count)).catchupAsNeeded();
    }

    private static LongArray initSeqsAndSlotBases(MemoryManager memMgr, int startValue, int count) {
        List<Long> values = new ArrayList<Long>();
        for (long i = startValue; i < startValue + count; i++) {
            values.add(i);
        }
        Collections.shuffle(values);

        LongArray seqsAndSlotBases = new LongArray(memMgr, count * 2);
        for (int i = 0, slot = 0; i < values.size(); i++) {
            seqsAndSlotBases.set(slot++, values.get(i));
            seqsAndSlotBases.set(slot++, valueAddr2slotBase(values.get(i)));
        }
        return seqsAndSlotBases;
    }
}
