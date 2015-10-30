package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.impl.gc.RecordMap.Cursor;
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
public class RecordMapTest extends OnHeapOffHeapTestBase {

    private RecordMap m;

    @Before public void setup() {
        m = offHeap ? new RecordMapOffHeap(malloc) : new RecordMapOnHeap();
    }

    @After public void destroy() {
        m.dispose();
    }

    @Test public void whenPutAbsent_thenCanGetIt() {
        assertNull(m.putIfAbsent(keyPrefix, keyHandle, 3, 4, true, 5));
        final Record r = m.get(keyHandle);
        assertEquals(keyPrefix, r.keyPrefix(keyHandle));
        assertEquals(3, r.liveSeq());
        assertEquals(4, r.size());
        assertTrue(r.isTombstone());
        assertEquals(5, r.garbageCount());
    }

    @Test public void whenPutExistingKey_thenExistingRecordReturned() {
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, true, 5);
        final Record r = m.putIfAbsent(keyPrefix, keyHandle, 13, 14, false, 15);
        assertNotNull(r);
        assertEquals(keyPrefix, r.keyPrefix(keyHandle));
        assertEquals(3, r.liveSeq());
        assertEquals(4, r.size());
        assertTrue(r.isTombstone());
        assertEquals(5, r.garbageCount());
    }

    @Test public void getThenUpdate_updatesInMap() {
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, true, 5);
        final Record retrievedBeforeUpdate = m.get(keyHandle);
        retrievedBeforeUpdate.decrementGarbageCount();
        retrievedBeforeUpdate.negateSeq();
        assertEquals(4, retrievedBeforeUpdate.garbageCount());
        assertEquals(-3, retrievedBeforeUpdate.rawSeqValue());
        final Record retrievedAfterUpdate = m.get(keyHandle);
        assertEquals(4, retrievedAfterUpdate.garbageCount());
        assertEquals(-3, retrievedAfterUpdate.rawSeqValue());
    }

    @Test public void size_reportsCorrectly() {
        assertEquals(0, m.size());
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, true, 5);
        assertEquals(1, m.size());
    }

    @Test public void cursor_traversesAll() {
        m.putIfAbsent(keyPrefix, keyHandle, 3, 4, true, 5);
        final int keyPrefix2 = keyPrefix + 1;
        m.putIfAbsent(keyPrefix2, keyHandle(keyPrefix2), 13, 14, false, 15);
        final Set<Long> seenPrefixes = new HashSet<Long>();
        for (Cursor c = m.cursor(); c.advance(); ) {
            seenPrefixes.add(c.asRecord().keyPrefix(c.toKeyHandle()));
        }
        assertEquals(2, seenPrefixes.size());
    }
}
