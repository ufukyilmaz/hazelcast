package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.elastic.map.HashSlotArrayImpl.valueAddr2slotBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SortedBySeqRecordCursorOffHeapTest {

    private final MemoryAllocator MALLOC = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));

    @Test(expected = NullPointerException.class)
    public void seqsAndSlotBasesAreNull() {
        new SortedBySeqRecordCursorOffHeap(null, MALLOC, mock(MutatorCatchup.class));
    }

    @Test
    public void initialisationAndIteration() {
        LongArray seqsAndSlotBases = new LongArray(MALLOC, 8);
        SortedBySeqRecordCursorOffHeap cursor = new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, MALLOC,
                mock(MutatorCatchup.class));

        assertTrue(cursor.advance());
    }

    @Test
    public void initialisationDisposeAndIteration() {
        LongArray seqsAndSlotBases = new LongArray(MALLOC, 8);
        SortedBySeqRecordCursorOffHeap cursor = new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, MALLOC,
                mock(MutatorCatchup.class));

        cursor.dispose();

        assertFalse(cursor.advance());
    }

    @Test
    public void recordsShouldBeOrderedBySequenceInCursor() {
        // GIVEN
        int count = 1 << 11;
        GcExecutor.MutatorCatchup mc = mock(MutatorCatchup.class);
        LongArray seqsAndSlotBases = initSeqsAndSlotBases(MALLOC, 1, count);

        // WHEN
        SortedBySeqRecordCursorOffHeap cursorOffHeap = new SortedBySeqRecordCursorOffHeap(
                seqsAndSlotBases, MALLOC, mc);

        // THEN
        int actualCount = 0;
        for (int expected = 1; cursorOffHeap.advance(); expected++, actualCount++) {
            RecordOffHeap rec = ((RecordOffHeap) cursorOffHeap.asRecord());
            assertEquals("wrong record address", expected, rec.address);
        }
        assertEquals("wrong elements count in cursor", actualCount, count);
    }

    @Test
    public void mutatorCatchupShouldBeCalledInCursorAtLeastCountTimes() {
        // GIVEN
        int count = 1 << 11;
        GcExecutor.MutatorCatchup mc = mock(MutatorCatchup.class);
        LongArray seqsAndSlotBases = initSeqsAndSlotBases(MALLOC, 1, count);

        // WHEN
        SortedBySeqRecordCursorOffHeap cursorOffHeap = new SortedBySeqRecordCursorOffHeap(
                seqsAndSlotBases, MALLOC, mc);

        // THEN
        assertNotNull(cursorOffHeap);
        verify(mc, atLeast(count)).catchupAsNeeded();
    }

    private static LongArray initSeqsAndSlotBases(MemoryAllocator malloc, int startValue, int count) {
        List<Long> values = new ArrayList<Long>();
        for (long i = startValue; i < startValue + count; i++) {
            values.add(i);
        }
        Collections.shuffle(values);

        LongArray seqsAndSlotBases = new LongArray(malloc, count * 2);
        for (int i = 0, slot = 0; i < values.size(); i++) {
            seqsAndSlotBases.set(slot++, values.get(i));
            seqsAndSlotBases.set(slot++, valueAddr2slotBase(values.get(i)));
        }
        return seqsAndSlotBases;
    }

}
