package com.hazelcast.spi.hotrestart.impl.gc.tracker;

import com.hazelcast.spi.hotrestart.impl.gc.AbstractOnHeapOffHeapTest;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap.Cursor;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class TrackerMapTest extends AbstractOnHeapOffHeapTest {
    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private TrackerMapBase m;

    @Before
    public void setup() {
        m = offHeap ? new TrackerMapOffHeap(memMgr, null) : new TrackerMapOnHeap();
    }

    @After
    @Override
    public void destroy() {
        m.dispose();
        super.destroy();
    }

    @Test
    public void whenPutAbsent_thenCanGetIt() {
        // When
        assertNull(m.putIfAbsent(keyHandle, 2, true));

        // Then
        final Tracker tr = m.get(keyHandle);
        assertEquals(2, tr.chunkSeq());
        assertTrue(tr.isTombstone());
        assertEquals(0, tr.garbageCount());
    }

    @Test
    public void whenPutAbsentValue_thenValueCountIncremented() {
        // When
        assertNull(m.putIfAbsent(keyHandle, 2, false));

        // Then
        assertEquals(1, m.liveValues.get());
        assertEquals(0, m.liveTombstones.get());
    }

    @Test
    public void whenPutAbsentTombstone_thenTombstoneCountIncremented() {
        // When
        assertNull(m.putIfAbsent(keyHandle, 2, true));

        // Then
        assertEquals(0, m.liveValues.get());
        assertEquals(1, m.liveTombstones.get());
    }

    @Test
    public void whenRemoveLiveTombstone_thenTombstoneCountDecremented() {
        // Given
        assertNull(m.putIfAbsent(keyHandle, 2, true));
        assertEquals(1, m.liveTombstones.get());

        // When
        m.removeLiveTombstone(keyHandle);

        // Then
        assertEquals(0, m.liveTombstones.get());
    }

    @Test
    public void whenRemoveIfDead_butAlive_thenDoNothing() {
        // Given
        assertNull(m.putIfAbsent(keyHandle, 2, false));

        // When
        m.removeIfDead(keyHandle, m.get(keyHandle));

        // Then
        assertNotNull(m.get(keyHandle));
    }

    @Test
    public void whenRemoveIfDead_andIsDead_thenRemoved() {
        // Given
        assertNull(m.putIfAbsent(keyHandle, 2, false));
        m.get(keyHandle).retire(m);

        // When
        m.removeIfDead(keyHandle, m.get(keyHandle));

        // Then
        assertNull(m.get(keyHandle));
    }

    @Test
    public void whenRetireValue_thenValueCountDecremented() {
        // Given
        assertNull(m.putIfAbsent(keyHandle, 2, false));
        assertEquals(1, m.liveValues.get());

        // When
        m.get(keyHandle).retire(m);

        // Then
        assertEquals(0, m.liveValues.get());
    }

    @Test
    public void whenRetireTombstone_thenTombstoneCountDecremented() {
        // Given
        assertNull(m.putIfAbsent(keyHandle, 2, true));
        assertEquals(1, m.liveTombstones.get());

        // When
        m.get(keyHandle).retire(m);

        // Then
        assertEquals(0, m.liveTombstones.get());
    }

    @Test
    public void whenPutExistingKey_thenExistingTrackerReturned() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);

        // When
        final Tracker tr = m.putIfAbsent(keyHandle, 12, false);

        // Then
        assertNotNull(tr);
        assertEquals(2, tr.chunkSeq());
        assertTrue(tr.isTombstone());
    }

    @Test
    public void whenGetThenUpdate_thenUpdateInMap() {
        // Given
        final int chunkSeqBeforeUpdate = 2;
        final int chunkSeqAfterUpdate = 15;
        final int garbageCountBeforeUpdate = 0;
        final int garbageCountAfterUpdate = 1;
        m.putIfAbsent(keyHandle, chunkSeqBeforeUpdate, false);
        final Tracker retrievedBeforeUpdate = m.get(keyHandle);
        assertEquals(garbageCountBeforeUpdate, retrievedBeforeUpdate.garbageCount());
        assertEquals(chunkSeqBeforeUpdate, retrievedBeforeUpdate.chunkSeq());

        // When
        retrievedBeforeUpdate.incrementGarbageCount();
        retrievedBeforeUpdate.moveToChunk(chunkSeqAfterUpdate);
        final Tracker retrievedAfterUpdate = m.get(keyHandle);

        // Then
        assertEquals(garbageCountAfterUpdate, retrievedAfterUpdate.garbageCount());
        assertEquals(chunkSeqAfterUpdate, retrievedAfterUpdate.chunkSeq());
    }

    @Test
    public void tracker_replaceLiveValueWithValue() {
        // Given
        m.putIfAbsent(keyHandle, 2, false);
        final Tracker tr = m.get(keyHandle);

        // When
        tr.newLiveRecord(1, false, m, false);

        // Then
        assertEquals(1, m.liveValues.get());
        assertEquals(0, m.liveTombstones.get());
    }

    @Test
    public void tracker_replaceDeadValueWithValue() {
        // Given
        m.putIfAbsent(keyHandle, 2, false);
        final Tracker tr = m.get(keyHandle);
        tr.retire(m);

        // When
        tr.newLiveRecord(1, false, m, false);

        // Then
        assertEquals(1, m.liveValues.get());
        assertEquals(0, m.liveTombstones.get());
    }

    @Test
    public void tracker_replaceLiveValueWithTombstone() {
        // Given
        m.putIfAbsent(keyHandle, 2, false);
        final Tracker tr = m.get(keyHandle);

        // When
        tr.newLiveRecord(1, true, m, false);

        // Then
        assertEquals(0, m.liveValues.get());
        assertEquals(1, m.liveTombstones.get());
    }

    @Test
    public void tracker_replaceLiveTombstoneWithValue() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        final Tracker tr = m.get(keyHandle);

        // When
        tr.newLiveRecord(1, false, m, false);

        // Then
        assertEquals(1, m.liveValues.get());
        assertEquals(0, m.liveTombstones.get());
    }

    @Test
    @RequireAssertEnabled
    public void tracker_replaceLiveTombstoneWithTombstone() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        final Tracker tr = m.get(keyHandle);

        // Then
        exceptionRule.expect(AssertionError.class);

        // When
        tr.newLiveRecord(1, true, m, false);
    }

    @Test
    @RequireAssertEnabled
    public void tracker_replaceLiveTombstoneWithTombstone_whileRestarting() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        final Tracker tr = m.get(keyHandle);

        // When
        tr.newLiveRecord(1, true, m, true);

        // Then
        assertEquals(0, m.liveValues.get());
        assertEquals(1, m.liveTombstones.get());
    }

    @Test
    @RequireAssertEnabled
    public void whenReduceGarbageCountBelowZero_thenAssertionError() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        final Tracker tr = m.get(keyHandle);

        // Then
        exceptionRule.expect(AssertionError.class);

        // When
        tr.reduceGarbageCount(1000);
    }

    @Test
    public void sizeMethod_worksAsExpected() {
        // Given
        assertEquals(0, m.size());

        // When
        m.putIfAbsent(keyHandle, 2, true);

        // Then
        assertEquals(1, m.size());
    }

    @Test
    public void cursor_traversesAll() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        m.putIfAbsent(keyHandle(keyPrefix + 1), 12, false);

        // When
        final Set<Long> seenChunkSeqs = new HashSet<Long>();
        for (Cursor c = m.cursor(); c.advance(); ) {
            seenChunkSeqs.add(c.asTracker().chunkSeq());
        }

        // Then
        assertEquals(2, seenChunkSeqs.size());
    }

    @Test
    public void cursor_toKeyHandle() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        final Cursor cursor = m.cursor();
        cursor.advance();

        // When - Then
        assertEquals(keyHandle, cursor.toKeyHandle());
    }

    @Test
    public void cursor_asKeyHandle() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);
        final Cursor cursor = m.cursor();
        cursor.advance();

        // When - Then
        assertEquals(keyHandle, cursor.asKeyHandle());
    }

    @Test
    public void tracker_toString_returnsSomeString() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);

        // When - Then
        assertNotNull(m.get(keyHandle).toString());
        assertNotNull(new TrackerOffHeap().toString());
    }

    @Test
    public void trackerMap_toString_returnsSomeString() {
        // Given
        m.putIfAbsent(keyHandle, 2, true);

        assertNotNull(m.toString());
    }
}
