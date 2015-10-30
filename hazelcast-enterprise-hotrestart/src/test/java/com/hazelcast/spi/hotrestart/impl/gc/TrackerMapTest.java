package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.gc.TrackerMap.Cursor;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TrackerMapTest extends OnHeapOffHeapTestBase {

    private TrackerMap m;

    @Before public void setup() {
        m = offHeap ? new TrackerMapOffHeap(malloc) : new TrackerMapOnHeap();
    }

    @After public void destroy() {
        m.dispose();
    }

    @Test public void whenPutAbsent_thenCanGetIt() {
        assertNull(m.putIfAbsent(keyHandle, 2, true));
        final Tracker tr = m.get(keyHandle);
        assertEquals(2, tr.chunkSeq());
        assertTrue(tr.isTombstone());
        assertEquals(0, tr.garbageCount());
    }

    @Test public void whenPutExistingKey_thenExistingTrackerReturned() {
        m.putIfAbsent(keyHandle, 2, true);
        final Tracker tr = m.putIfAbsent(keyHandle, 12, false);
        assertNotNull(tr);
        assertEquals(2, tr.chunkSeq());
        assertTrue(tr.isTombstone());
    }

    @Test public void getThenUpdate_shouldUpdateInMap() {
        m.putIfAbsent(keyHandle, 2, true);
        final Tracker retrievedBeforeUpdate = m.get(keyHandle);
        retrievedBeforeUpdate.incrementGarbageCount();
        retrievedBeforeUpdate.moveToChunk(12);
        assertEquals(1, retrievedBeforeUpdate.garbageCount());
        assertEquals(12, retrievedBeforeUpdate.chunkSeq());
        final Tracker retrievedAfterUpdate = m.get(keyHandle);
        assertEquals(1, retrievedAfterUpdate.garbageCount());
        assertEquals(12, retrievedAfterUpdate.chunkSeq());
    }

    @Test public void sizeMethodShouldWorkAsExpected() {
        assertEquals(0, m.size());
        m.putIfAbsent(keyHandle, 2, true);
        assertEquals(1, m.size());
    }

    @Test public void cursorShouldTraverseAll() {
        m.putIfAbsent(keyHandle, 2, true);
        m.putIfAbsent(keyHandle(keyPrefix + 1), 12, false);
        final Set<Long> seenChunkSeqs = new HashSet<Long>();
        for (Cursor c = m.cursor(); c.advance(); ) {
            seenChunkSeqs.add(c.asTracker().chunkSeq());
        }
        assertEquals(2, seenChunkSeqs.size());
    }
}
