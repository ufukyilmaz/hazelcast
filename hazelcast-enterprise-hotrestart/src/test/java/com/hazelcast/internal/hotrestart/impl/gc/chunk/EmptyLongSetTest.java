package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.EmptyLongSet.emptyLongSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EmptyLongSetTest {
    private final EmptyLongSet es = emptyLongSet();

    @Test
    public void allInOneTest() {
        // When - Then
        try {
            assertFalse(es.add(1));
            fail();
        } catch (UnsupportedOperationException ignored) {
        }
        assertFalse(es.remove(1));
        assertFalse(es.contains(1));
        assertEquals(0, es.size());
        assertTrue(es.isEmpty());
        es.clear();
        assertSame(es, es.cursor());
        es.dispose();
        assertFalse(es.advance());
        es.reset();
        try {
            es.value();
            fail();
        } catch (AssertionError ignored) {
        }
    }
}
