package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.AbstractOnHeapOffHeapTest;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap.Cursor;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOffHeap.newRecordMapOffHeap;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOffHeap.newTombstoneMapOffHeap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordMapTest extends AbstractOnHeapOffHeapTest {

    private RecordMap m;

    @Before
    public void setup() {
        m = offHeap ? newRecordMapOffHeap(memMgr, memMgr) : new RecordMapOnHeap();
    }

    @After
    @Override
    public void destroy() {
        m.dispose();
        super.destroy();
    }

    @Test
    public void newTombstoneMapOffHeap_returnsSomeMap() {
        if (!offHeap) {
            return;
        }
        assertNotNull(newTombstoneMapOffHeap(memMgr));
    }

    @Test
    public void whenGetAbsent_thenReturnNull() {
        assertNull(m.get(keyHandle));
    }

    @Test
    public void whenPutAbsent_thenCanGetIt() {
        // When
        assertNull(m.putIfAbsent(keyPrefix, keyHandle, 3, 4, false, 5));

        // Then
        final Record r = m.get(keyHandle);
        assertEquals(keyPrefix, r.keyPrefix(keyHandle));
        assertEquals(3, r.liveSeq());
        assertEquals(4, r.size());
        assertFalse(r.isTombstone());
        assertEquals(5, r.garbageCount());
    }

    @Test
    public void whenPutExistingKey_thenExistingRecordReturned() {
        // Given
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, false, 5);

        // When
        final Record r = m.putIfAbsent(keyPrefix, keyHandle, 13, 14, false, 15);

        // Then
        assertNotNull(r);
        assertEquals(keyPrefix, r.keyPrefix(keyHandle));
        assertEquals(3, r.liveSeq());
        assertEquals(4, r.size());
        assertFalse(r.isTombstone());
        assertEquals(5, r.garbageCount());
    }

    @Test
    public void getThenUpdate_updatesInMap() {
        // Given
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, false, 5);
        final Record retrievedBeforeUpdate = m.get(keyHandle);
        retrievedBeforeUpdate.decrementGarbageCount();
        retrievedBeforeUpdate.negateSeq();
        assertEquals(4, retrievedBeforeUpdate.garbageCount());
        assertEquals(-3, retrievedBeforeUpdate.rawSeqValue());

        // When
        final Record retrievedAfterUpdate = m.get(keyHandle);

        // Then
        assertEquals(4, retrievedAfterUpdate.garbageCount());
        assertEquals(-3, retrievedAfterUpdate.rawSeqValue());
    }

    @Test
    public void size_reportsCorrectly() {
        // Given
        assertEquals(0, m.size());

        // When
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, true, 5);

        // Then
        assertEquals(1, m.size());
    }

    @Test
    public void cursor_traversesAll() {
        // Given
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, false, 5);
        final int keyPrefix2 = keyPrefix + 1;
        m.putIfAbsent(keyPrefix2, keyHandle(keyPrefix2), 13, 14, false, 15);

        // When
        final Set<Long> seenPrefixes = new HashSet<Long>();
        for (Cursor c = m.cursor(); c.advance(); ) {
            seenPrefixes.add(c.asRecord().keyPrefix(c.toKeyHandle()));
        }

        // Then
        assertEquals(2, seenPrefixes.size());
    }

    @Test
    public void sortedBySeqCursor_traversesAllInOrder() {
        // Given
        final int recordCount = 100;
        for (int seq = recordCount; seq >= 1; seq--) {
            final int prefix = keyPrefix + seq;
            m.putIfAbsent(prefix, keyHandle(prefix), seq, 10, false, 0);
        }

        // When
        final SortedBySeqRecordCursor cursor =
                m.sortedBySeqCursor(recordCount, new RecordMap[]{m}, Mockito.mock(MutatorCatchup.class));

        // Then
        long prevSeq = -1;
        long traversedCount = 0;
        while (cursor.advance()) {
            final long seq = cursor.asRecord().liveSeq();
            assertTrue(seq > prevSeq);
            prevSeq = seq;
            traversedCount++;
        }
        assertEquals(recordCount, traversedCount);
    }

    @Test
    public void toStable_returnsMapWithSameContents() {
        // Given
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, false, 5);
        final int keyPrefix2 = keyPrefix + 1;
        m.putIfAbsent(keyPrefix2, keyHandle(keyPrefix2), 13, 14, false, 15);

        // When
        final RecordMap stable = m.toStable();

        // Then
        assertNotNull(stable.get(keyHandle));
        assertNotNull(stable.get(keyHandle(keyPrefix2)));
    }
}
