package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle.KhCursor;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.gc.AbstractOnHeapOffHeapTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetOfKeyHandleTest extends AbstractOnHeapOffHeapTest {

    private SetOfKeyHandle set;

    @Before
    public void setup() {
        set = offHeap ? new SetOfKeyHandleOffHeap(memMgr) : new SetOfKeyHandleOnHeap();
    }

    @After
    @Override
    public void destroy() {
        set.dispose();
        super.destroy();
    }

    @Test
    public void when_addTwice_then_containsItOnce() {
        // When
        set.add(keyHandle);
        set.add(keyHandle);
        final KhCursor cursor = set.cursor();

        // Then
        assertTrue(cursor.advance());
        assertEquals(keyHandle, cursor.asKeyHandle());
        assertFalse(cursor.advance());
    }

    @Test
    public void when_addAndRemove_then_empty() {
        // When
        set.add(keyHandle);
        set.remove(keyHandle);

        // Then
        assertFalse(set.cursor().advance());
    }

    @Test
    public void cursorEqualsAndHashCode_workWell() {
        // Given
        final int address = 13;
        final int sequenceId = 17;
        final KeyHandleOffHeap kh = new SimpleHandleOffHeap(address, sequenceId);
        set.add(kh);

        // When
        final KhCursor cursor = set.cursor();
        cursor.advance();
        final KeyHandle cursorKh = cursor.asKeyHandle();

        // Then
        assertTrue(cursorKh.equals(kh));
        assertEquals(kh.hashCode(), cursorKh.hashCode());
    }
}
