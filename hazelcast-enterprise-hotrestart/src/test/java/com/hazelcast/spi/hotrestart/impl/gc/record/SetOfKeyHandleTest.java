package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle.KhCursor;
import com.hazelcast.spi.hotrestart.impl.gc.OnHeapOffHeapTestBase;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetOfKeyHandleTest extends OnHeapOffHeapTestBase {

    private SetOfKeyHandle set;

    @Before public void setup() {
        set = offHeap ? new SetOfKeyHandleOffHeap(memMgr) : new SetOfKeyHandleOnHeap();
    }

    @After public void destroy() {
        set.dispose();
    }

    @Test public void whenAddTwice_thenContainsItOnce() {
        set.add(keyHandle);
        set.add(keyHandle);
        final KhCursor cursor = set.cursor();
        assertTrue(cursor.advance());
        assertEquals(keyHandle, cursor.asKeyHandle());
        assertFalse(cursor.advance());
    }

    @Test public void whenAddAndRemove_thenEmpty() {
        set.add(keyHandle);
        set.remove(keyHandle);
        final KhCursor cursor = set.cursor();
        assertFalse(cursor.advance());
    }
}
