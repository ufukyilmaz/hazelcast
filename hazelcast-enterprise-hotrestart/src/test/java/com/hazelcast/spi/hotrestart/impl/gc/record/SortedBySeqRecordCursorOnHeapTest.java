package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SortedBySeqRecordCursorOnHeapTest {

    @Test(expected = NullPointerException.class)
    public void handlesAreNull() {
        new SortedBySeqRecordCursorOnHeap(null, Collections.<Record>emptyList(), mock(MutatorCatchup.class));
    }

    @Test(expected = NullPointerException.class)
    public void recordsAreNull() {
        new SortedBySeqRecordCursorOnHeap(Collections.<KeyHandle>emptyList(), null, mock(MutatorCatchup.class));
    }

    @Test(expected = AssertionError.class)
    public void when_keyHandlesAndRecordsSizeMismatch_then_assertionError() {
        new SortedBySeqRecordCursorOnHeap(singletonList(keyHandle(1)), Collections.<Record>emptyList(),
                mock(MutatorCatchup.class));
    }

    @Test
    public void when_advanceOnEmpty_then_returnFalse() {
        // Given
        SortedBySeqRecordCursorOnHeap cursor = new SortedBySeqRecordCursorOnHeap(Collections.<KeyHandle>emptyList(),
                Collections.<Record>emptyList(), mock(MutatorCatchup.class));

        // When - Then
        assertFalse(cursor.advance());
    }

    @Test
    public void records_orderedBySequence() {
        // GIVEN
        int count = 1 << 12;
        MutatorCatchup mc = mock(MutatorCatchup.class);
        TestKeyHandleRecordPairs testPairs = new TestKeyHandleRecordPairs(count);

        // WHEN
        SortedBySeqRecordCursorOnHeap cursorOnHeap = new SortedBySeqRecordCursorOnHeap(testPairs.shuffledKeyHandles,
                testPairs.shuffledRecords, mc);

        // THEN
        int actualCount = 0;
        for (int seq = 1; cursorOnHeap.advance(); seq++, actualCount++) {
            KeyHandleRecordPair pair = testPairs.get(seq);
            assertEquals("wrong handle for sequence " + seq, pair.keyHandle, cursorOnHeap.asKeyHandle());
            assertEquals("wrong record for sequence " + seq, pair.record, cursorOnHeap.asRecord());
        }
        assertEquals("wrong elements count in cursor", actualCount, count);
    }

    @Test
    public void mutatorCatchup_calledInCursorAtLeastCountTimes() {
        // GIVEN
        int count = 1 << 12;
        MutatorCatchup mc = mock(MutatorCatchup.class);
        TestKeyHandleRecordPairs testPairs = new TestKeyHandleRecordPairs(count);

        // WHEN
        SortedBySeqRecordCursorOnHeap cursorOnHeap = new SortedBySeqRecordCursorOnHeap(testPairs.shuffledKeyHandles,
                testPairs.shuffledRecords, mc);

        // THEN
        assertNotNull(cursorOnHeap);
        verify(mc, atLeast(count)).catchupAsNeeded();
    }

    @Test
    public void dispose_doesntDoAThing() {
        new SortedBySeqRecordCursorOnHeap(
                Collections.<KeyHandle>emptyList(), Collections.<Record>emptyList(), mock(MutatorCatchup.class))
                .dispose();
    }

    protected KeyHandle keyHandle(int mockData) {
        return new KeyOnHeap(mockData, new byte[1]);
    }

    private static class TestKeyHandleRecordPairs {
        final List<KeyHandleRecordPair> orderedPairs = new ArrayList<KeyHandleRecordPair>();
        final List<KeyHandle> shuffledKeyHandles = new ArrayList<KeyHandle>();
        final List<Record> shuffledRecords = new ArrayList<Record>();

        TestKeyHandleRecordPairs(int count) {
            for (int i = 1; i <= count; i++) {
                KeyHandleRecordPair pair = new KeyHandleRecordPair();
                pair.keyHandle = new KeyOnHeap(1, new byte[1]);
                pair.record = new RecordOnHeap(i, 10, false, 4);
                orderedPairs.add(pair);
            }
            List<KeyHandleRecordPair> shuffledPairs = new ArrayList<KeyHandleRecordPair>(orderedPairs);
            Collections.shuffle(shuffledPairs);
            for (KeyHandleRecordPair pair : shuffledPairs) {
                shuffledKeyHandles.add(pair.keyHandle);
                shuffledRecords.add(pair.record);
            }
        }

        KeyHandleRecordPair get(long seq) {
            return orderedPairs.get((int) (seq - 1));
        }
    }

    private static class KeyHandleRecordPair {
        KeyHandle keyHandle;
        RecordOnHeap record;
    }
}
