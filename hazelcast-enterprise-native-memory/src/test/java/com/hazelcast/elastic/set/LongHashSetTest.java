package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.JvmMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongHashSetTest {

    private final Random random = new Random();
    private JvmMemoryManager memoryManager;
    private LongHashSet set;

    @Before
    public void setUp() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        set = new LongHashSet(memoryManager, 0L);
    }

    @After
    public void tearDown() throws Exception {
        set.dispose();
        memoryManager.dispose();
    }

    @Test
    public void testAdd() throws Exception {
        long key = random.nextLong();
        assertTrue(set.add(key));
        assertFalse(set.add(key));
    }

    @Test
    public void testAddMany() throws Exception {
        for (int i = 1; i <= 1000; i++) {
            assertTrue(set.add(i));
        }
    }

    @Test
    public void testRemove() throws Exception {
        long key = random.nextLong();
        assertFalse(set.remove(key));
        set.add(key);
        assertTrue(set.remove(key));
    }

    @Test
    public void testContains() throws Exception {
        long key = random.nextLong();
        assertFalse(set.contains(key));
        set.add(key);
        assertTrue(set.contains(key));
    }

    @Test
    public void testIterator() throws Exception {
        assertFalse(set.iterator().hasNext());

        Set<Long> expected = new HashSet<Long>();
        for (int i = 1; i <= 1000; i++) {
            set.add(i);
            expected.add((long) i);
        }

        LongIterator iterator = set.iterator();
        while (iterator.hasNext()) {
            long key = iterator.next();
            assertTrue("Key: " + key, expected.remove(key));

            iterator.remove();
        }

        assertTrue("size: " + set.size(), set.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        for (int i = 1; i <= 1000; i++) {
            set.add(i);
        }

        set.clear();
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(0, set.size());

        int expected = 1000;
        for (int i = 1; i <= expected; i++) {
            set.add(i);
        }

        assertEquals(expected, set.size());
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertTrue(set.isEmpty());

        set.add(random.nextLong());

        assertFalse(set.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void testAdd_after_destroy() throws Exception {
        set.dispose();
        set.add(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testRemove_after_destroy() throws Exception {
        set.dispose();
        set.remove(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testContains_after_destroy() throws Exception {
        set.dispose();
        set.contains(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testIterator_after_destroy() throws Exception {
        set.dispose();
        set.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void testIterator_after_destroy2() throws Exception {
        set.add(1);
        LongIterator iterator = set.iterator();
        set.dispose();
        iterator.next();
    }
}
