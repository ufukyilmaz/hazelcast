package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
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
    private MemoryManager memoryManager;
    private LongHashSet set;

    @Before
    public void setUp() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        set = new LongHashSet(memoryManager);
    }

    @After
    public void tearDown() throws Exception {
        set.destroy();
        memoryManager.destroy();
    }

    @Test
    public void testAdd() throws Exception {
        long key = random.nextLong();
        Assert.assertTrue(set.add(key));
        Assert.assertFalse(set.add(key));
    }

    @Test
    public void testAddMany() throws Exception {
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(set.add(i));
        }
    }

    @Test
    public void testRemove() throws Exception {
        long key = random.nextLong();
        Assert.assertFalse(set.remove(key));
        set.add(key);
        Assert.assertTrue(set.remove(key));
    }

    @Test
    public void testContains() throws Exception {
        long key = random.nextLong();
        Assert.assertFalse(set.contains(key));
        set.add(key);
        Assert.assertTrue(set.contains(key));
    }

    @Test
    public void testIterator() throws Exception {
        assertFalse(set.iterator().hasNext());

        Set<Long> expected = new HashSet<Long>();
        for (int i = 0; i < 1000; i++) {
            set.add(i);
            expected.add((long) i);
        }

        LongIterator iterator = set.iterator();
        while (iterator.hasNext()) {
            long key = iterator.next();
            assertTrue("Key: " + key, expected.remove(key));

            iterator.remove();
        }

        assertTrue(set.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        for (int i = 0; i < 1000; i++) {
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
        for (int i = 0; i < expected; i++) {
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
        set.destroy();
        set.add(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testRemove_after_destroy() throws Exception {
        set.destroy();
        set.remove(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testContains_after_destroy() throws Exception {
        set.destroy();
        set.contains(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testIterator_after_destroy() throws Exception {
        set.destroy();
        set.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void testIterator_after_destroy2() throws Exception {
        set.add(1);
        LongIterator iterator = set.iterator();
        set.destroy();
        iterator.next();
    }
}
