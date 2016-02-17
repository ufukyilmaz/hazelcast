package com.hazelcast.elastic.map.long2long;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class Long2LongElasticMapHsaTest {

    private static final long MISSING_VALUE = -1L;

    private final Random random = new Random();
    private MemoryManager memoryManager;
    private Long2LongElasticMapHsa map;

    @Before
    public void setUp() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        map = new Long2LongElasticMapHsa(MISSING_VALUE, memoryManager);
    }

    @After
    public void tearDown() throws Exception {
        map.dispose();
        memoryManager.destroy();
    }

    private long newKey() {
        return random.nextLong();
    }

    private long newKey(int keyRange) {
        return (long) random.nextInt(keyRange);
    }

    private long newValue() {
        return random.nextInt(Integer.MAX_VALUE) + 1;
    }

    @Test
    public void testPut() {
        long key = newKey();
        long value = newValue();
        assertEquals(MISSING_VALUE, map.put(key, value));

        long newValue = newValue();
        long oldValue = map.put(key, newValue);
        assertEquals(value, oldValue);
    }

    @Test
    public void testGet() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long currentValue = map.get(key);
        assertEquals(value, currentValue);
    }

    @Test
    public void testPutIfAbsent_success() {
        long key = newKey();
        long value = newValue();
        assertEquals(MISSING_VALUE, map.putIfAbsent(key, value));
    }

    @Test
    public void testPutIfAbsent_fail() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long newValue = newValue();
        assertEquals(value, map.putIfAbsent(key, newValue));
    }

    @Test
    public void testPutAll() throws Exception {
        int count = 100;
        Long2LongElasticMap entries = new Long2LongElasticMapHsa(count, memoryManager);

        for (int i = 0; i < count; i++) {
            long key = newKey();
            long value = newValue();
            entries.put(key, value);
        }

        map.putAll(entries);
        assertEquals(count, map.size());

        for (LongLongCursor cursor = map.cursor(); cursor.advance();) {
            assertEquals(map.get(cursor.key()), cursor.value());
        }
    }

    @Test
    public void testReplace() throws Exception {
        long key = newKey();
        long value = newValue();

        assertEquals(MISSING_VALUE, map.replace(key, value));

        map.put(key, value);

        long newValue = newValue();
        assertEquals(value, map.replace(key, newValue));
    }

    @Test
    public void testReplace_if_same_success() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long newValue = newValue();
        assertTrue(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_not_exist() {
        long key = newKey();
        long value = newValue();
        long newValue = newValue();
        assertFalse(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_fail() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long wrongValue = value + 1;
        long newValue = newValue();
        assertFalse(map.replace(key, wrongValue, newValue));
    }

    @Test
    public void testRemove() {
        long key = newKey();
        assertEquals(MISSING_VALUE, map.remove(key));

        long value = newValue();
        map.put(key, value);

        long oldValue = map.remove(key);
        assertEquals(value, oldValue);
    }

    @Test
    public void testRemove_if_same_success() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        assertTrue(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_not_exist() {
        long key = newKey();
        long value = newValue();
        assertFalse(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_fail() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long wrongValue = value + 1;
        assertFalse(map.remove(key, wrongValue));
    }

    @Test
    public void testContainsKey_fail() throws Exception {
        long key = newKey();
        assertFalse(map.containsKey(key));
    }

    @Test
    public void testContainsKey_success() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        assertTrue(map.containsKey(key));
    }

    @Test
    public void testPut_withTheSameValue() {
        long key = newKey();
        long value = newValue();
        map.put(key, value);

        long oldValue = map.put(key, value);
        assertEquals(value, oldValue);
    }

    @Test(expected = AssertionError.class)
    public void test_put_invalidValue() {
        map.put(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void test_putIfAbsent_invalidValue() {
        map.putIfAbsent(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void test_set_invalidValue() {
        map.put(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void test_replace_invalidValue() {
        map.replace(newKey(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void test_replaceIfEquals_invalidOldValue() {
        map.replace(newKey(), MISSING_VALUE, newValue());
    }

    @Test(expected = AssertionError.class)
    public void test_replaceIfEquals_invalidNewValue() {
        map.replace(newKey(), newValue(), MISSING_VALUE);
    }

    @Test(expected = AssertionError.class)
    public void test_removeIfEquals_Value() {
        map.remove(newKey(), MISSING_VALUE);
    }

    @Test
    public void testSize() {
        assertEquals(0, map.size());

        int expected = 100;
        for (long i = 0; i < expected; i++) {
            long key = i;
            long value = newValue();
            map.put(key, value);
        }

        assertEquals(expected, map.size());
    }

    @Test
    public void testClear() {
        for (long i = 0; i < 100; i++) {
            long key = i;
            long value = newValue();
            map.put(key, value);
        }

        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(map.isEmpty());

        long key = newKey();
        long value = newValue();
        map.put(key, value);

        assertFalse(map.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void testGet_after_dispose() {
        map.dispose();
        map.get(newKey());
    }

    @Test(expected = IllegalStateException.class)
    public void testPut_after_dispose() {
        map.dispose();
        map.put(newKey(), newValue());
    }

    @Test(expected = IllegalStateException.class)
    public void testRemove_after_dispose() {
        map.dispose();
        map.remove(newKey());
    }

    @Test(expected = IllegalStateException.class)
    public void testReplace_after_dispose() {
        map.dispose();
        map.replace(newKey(), newValue());
    }

    @Test(expected = IllegalStateException.class)
    public void testContainsKey_after_dispose() {
        map.dispose();
        map.containsKey(newKey());
    }

    @Test
    public void testMemoryLeak() {
        int keyRange = 100;

        for (int i = 0; i < 100000; i++) {
            int k = random.nextInt(7);
            switch (k) {
                case 0:
                    _put_(keyRange);
                    break;

                case 1:
                    _set_(keyRange);
                    break;

                case 2:
                    _putIfAbsent_(keyRange);
                    break;

                case 3:
                    _replace_(keyRange);
                    break;

                case 4:
                    _replaceIfSame_(keyRange);
                    break;

                case 5:
                    _remove_(keyRange);
                    break;

                case 6:
                    _removeIfPresent_(keyRange);
                    break;
            }
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

    private void _put_(int keyRange) {
        map.put(newKey(keyRange), newValue());
    }

    private void _set_(int keyRange) {
        map.put(newKey(keyRange), newValue());
    }

    private void _putIfAbsent_(int keyRange) {
        map.putIfAbsent(newKey(keyRange), newValue());
    }

    private void _replace_(int keyRange) {
        map.replace(newKey(keyRange), newValue());
    }

    private void _replaceIfSame_(int keyRange) {
        long key = newKey(keyRange);
        long value = newValue();
        long old = map.get(key);
        if (old != MISSING_VALUE) {
            map.replace(key, old, value);
        }
    }

    private void _remove_(int keyRange) {
        map.remove(newKey(keyRange));
    }

    private void _removeIfPresent_(int keyRange) {
        long key = newKey(keyRange);
        long old = map.get(key);
        if (old != MISSING_VALUE) {
            map.remove(key, old);
        }
    }

    @Test
    public void testDestroyMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.put(newKey(), newValue());
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

    @Test
    public void testMemoryLeak_whenCapacityExpandFails() {
        while (true) {
            // key is on-heap
            long key = newKey();
            long value = newValue();
            try {
                map.put(key, value);
            } catch (NativeOutOfMemoryError e) {
                break;
            }
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

}
