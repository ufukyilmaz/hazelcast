package com.hazelcast.elastic.map.long2long;

import com.hazelcast.elastic.CapacityUtil;
import com.hazelcast.elastic.SlottableIterator;
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class Long2LongElasticHashMapTest {

    private static final Long MISSING_VALUE = -1L;

    private final Random random = new Random();
    private MemoryManager memoryManager;
    private Long2LongElasticHashMap map;

    @Before
    public void setUp() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        map = new Long2LongElasticHashMap(memoryManager, MISSING_VALUE);
    }

    @After
    public void tearDown() throws Exception {
        if (map.capacity() > 0) {
            map.clear();
        }
        map.dispose();
        memoryManager.destroy();
    }

    private Long newKey() {
        return random.nextLong();
    }

    private Long newKey(int keyRange) {
        return (long) random.nextInt(keyRange);
    }

    private Long newValue() {
        return random.nextLong();
    }

    @Test
    public void testPut() {
        Long key = newKey();
        Long value = newValue();
        assertEquals(MISSING_VALUE, map.put(key, value));

        Long newValue = newValue();
        Long oldValue = map.put(key, newValue);
        assertEquals(value, oldValue);
    }

    @Test
    public void testSet() {
        Long key = newKey();
        Long value = newValue();
        assertTrue(map.set(key, value));

        Long newValue = newValue();
        assertFalse(map.set(key, newValue));
    }

    @Test
    public void testGet() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        Long currentValue = map.get(key);
        assertEquals(value, currentValue);
    }

    @Test
    public void testPutIfAbsent_success() {
        Long key = newKey();
        Long value = newValue();
        assertEquals(MISSING_VALUE, map.putIfAbsent(key, value));
    }

    @Test
    public void testPutIfAbsent_fail() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        Long newValue = newValue();
        assertEquals(value, map.putIfAbsent(key, newValue));
    }

    @Test
    public void testPutAll() throws Exception {
        int count = 100;
        Map<Long, Long> entries = new HashMap<Long, Long>(count);

        for (int i = 0; i < count; i++) {
            Long key = newKey();
            Long value = newValue();
            entries.put(key, value);
        }

        map.putAll(entries);
        assertEquals(count, map.size());

        for (Entry<Long, Long> entry : entries.entrySet()) {
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    public void testReplace() throws Exception {
        Long key = newKey();
        Long value = newValue();

        assertEquals(MISSING_VALUE, map.replace(key, value));

        map.set(key, value);

        Long newValue = newValue();
        assertEquals(value, map.replace(key, newValue));
    }

    @Test
    public void testReplace_if_same_success() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        Long newValue = newValue();
        assertTrue(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_not_exist() {
        Long key = newKey();
        Long value = newValue();
        Long newValue = newValue();
        assertFalse(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_fail() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        Long wrongValue = value + 1;
        Long newValue = newValue();
        assertFalse(map.replace(key, wrongValue, newValue));
    }

    @Test
    public void testRemove() {
        Long key = newKey();
        assertEquals(MISSING_VALUE, map.remove(key));

        Long value = newValue();
        map.set(key, value);

        Long oldValue = map.remove(key);
        assertEquals(value, oldValue);
    }

    @Test
    public void testDelete() {
        Long key = newKey();
        assertFalse(map.delete(key));

        Long value = newValue();
        map.set(key, value);

        assertTrue(map.delete(key));
    }

    @Test
    public void testRemove_if_same_success() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        assertTrue(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_not_exist() {
        Long key = newKey();
        Long value = newValue();
        assertFalse(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_fail() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        Long wrongValue = value + 1;
        assertFalse(map.remove(key, wrongValue));
    }

    @Test
    public void testContainsKey_fail() throws Exception {
        Long key = newKey();
        assertFalse(map.containsKey(key));
    }

    @Test
    public void testContainsKey_success() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_fail() {
        Long value = newValue();
        assertFalse(map.containsValue(value));
    }

    @Test
    public void testContainsValue_success() {
        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

        assertTrue(map.containsValue(value));
    }

    @Test
    public void testPut_withTheSameValue() {
        Long key = newKey();
        Long value = newValue();
        map.put(key, value);

        Long oldValue = map.put(key, value);
        assertEquals(value, oldValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_get_invalidKey() {
        map.get(MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_containsKey_invalidKey() {
        map.containsKey(MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_containsKey_invalidValue() {
        map.containsValue(MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_put_invalidKey() {
        map.put(MISSING_VALUE, newValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_put_invalidValue() {
        map.put(newKey(), MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putIfAbsent_invalidKey() {
        map.putIfAbsent(MISSING_VALUE, newValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_putIfAbsent_invalidValue() {
        map.putIfAbsent(newKey(), MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_set_invalidKey() {
        map.set(MISSING_VALUE, newValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_set_invalidValue() {
        map.set(newKey(), MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_replace_invalidKey() {
        map.replace(MISSING_VALUE, newValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_replace_invalidValue() {
        map.replace(newKey(), MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_replaceIfEquals_invalidKey() {
        map.replace(MISSING_VALUE, newValue(), newValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_replaceIfEquals_invalidOldValue() {
        map.replace(newKey(), MISSING_VALUE, newValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_replaceIfEquals_invalidNewValue() {
        map.replace(newKey(), newValue(), MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_remove_invalidKey() {
        map.remove(MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_removeIfEquals_Value() {
        map.remove(newKey(), MISSING_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_delete_invalidKey() {
        map.delete(MISSING_VALUE);
    }

    @Test
    public void test_capacity() {
        assertEquals(CapacityUtil.DEFAULT_CAPACITY, map.capacity());
    }

    @Test
    public void test_capacity_after_dispose() {
        map.dispose();
        assertEquals(0, map.capacity());
    }

    @Test
    public void testKeySet() {
        Set<Long> keys = map.keySet();
        assertTrue(keys.isEmpty());

        Set<Long> expected = new HashSet<Long>();
        for (long i = 0; i < 100; i++) {
            Long key = i;
            map.set(key, newValue());
            expected.add(key);
        }

        keys = map.keySet();
        assertEquals(expected.size(), keys.size());
        assertEquals(expected, keys);
    }

    @Test
    public void testKeySet_iterator() {
        Set<Long> keys = map.keySet();
        assertFalse(keys.iterator().hasNext());

        Set<Long> expected = new HashSet<Long>();
        for (long i = 0; i < 100; i++) {
            Long key = i;
            map.set(key, newValue());
            expected.add(key);
        }

        keys = map.keySet();
        assertEquals(expected.size(), keys.size());

        Iterator<Long> iter = keys.iterator();
        while (iter.hasNext()) {
            Long key = iter.next();
            assertNotNull(key);
            assertTrue(expected.contains(key));

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test
    public void testValues() {
        Collection<Long> values = map.values();
        assertTrue(values.isEmpty());

        Set<Long> expected = new HashSet<Long>();
        for (long i = 0; i < 100; i++) {
            Long key = i;
            Long value = newValue();
            map.set(key, value);
            expected.add(value);
        }

        values = map.values();

        assertEquals(expected.size(), values.size());
        assertTrue(expected.containsAll(values));
        assertTrue(values.containsAll(expected));
    }

    @Test
    public void testValues_iterator() {
        Collection<Long> values = map.values();
        assertFalse(values.iterator().hasNext());

        Set<Long> expected = new HashSet<Long>();
        for (long i = 0; i < 100; i++) {
            Long key = i;
            Long value = newValue();
            map.set(key, value);
            expected.add(value);
        }

        values = map.values();
        assertEquals(expected.size(), values.size());

        Iterator<Long> iter = values.iterator();
        while (iter.hasNext()) {
            Long value = iter.next();
            assertNotNull(value);
            assertTrue(expected.contains(value));

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test
    public void testEntrySet() {
        Set<Entry<Long, Long>> entries = map.entrySet();
        assertTrue(entries.isEmpty());

        Map<Long, Long> expected = new HashMap<Long, Long>();
        for (long i = 0; i < 100; i++) {
            Long key = i;
            Long value = newValue();
            map.set(key, value);
            expected.put(key, value);
        }

        entries = map.entrySet();
        assertEquals(expected.size(), entries.size());
        assertEquals(expected.entrySet(), entries);
    }

    @Test
    public void testEntrySet_iterator() {
        Set<Entry<Long, Long>> entries = map.entrySet();
        assertFalse(entries.iterator().hasNext());

        Map<Long, Long> expected = new HashMap<Long, Long>();
        for (long i = 0; i < 100; i++) {
            Long key = i;
            Long value = newValue();
            map.set(key, value);
            expected.put(key, value);
        }

        entries = map.entrySet();
        assertEquals(expected.size(), entries.size());

        Iterator<Entry<Long, Long>> iter = entries.iterator();
        while (iter.hasNext()) {
            Entry<Long, Long> entry = iter.next();
            assertNotNull(entry);
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());

            assertEquals(expected.get(entry.getKey()), entry.getValue());

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(0, map.size());

        int expected = 100;
        for (long i = 0; i < expected; i++) {
            Long key = i;
            Long value = newValue();
            map.set(key, value);
        }

        assertEquals(expected, map.size());
    }

    @Test
    public void testClear() {
        for (long i = 0; i < 100; i++) {
            Long key = i;
            Long value = newValue();
            map.set(key, value);
        }

        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(map.isEmpty());

        Long key = newKey();
        Long value = newValue();
        map.set(key, value);

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

    @Test(expected = IllegalStateException.class)
    public void testContainsValue_after_dispose() {
        map.dispose();
        map.containsValue(newValue());
    }

    @Test(expected = IllegalStateException.class)
    public void testKeySet_after_dispose() {
        map.dispose();
        map.keySet();
    }

    @Test(expected = IllegalStateException.class)
    public void testEntrySet_after_dispose() {
        map.dispose();
        map.entrySet();
    }

    @Test(expected = IllegalStateException.class)
    public void testValues_after_dispose() {
        map.dispose();
        map.values();
    }

    @Test(expected = IllegalStateException.class)
    public void testKeySet_iterator_after_dispose() {
        map.set(newKey(), newValue());
        Iterator<Long> iterator = map.keySet().iterator();
        map.dispose();
        iterator.next();
    }

    @Test(expected = IllegalStateException.class)
    public void testEntrySet_iterator_after_dispose() {
        map.set(newKey(), newValue());
        Iterator<Entry<Long, Long>> iterator = map.entrySet().iterator();
        map.dispose();
        iterator.next();
    }

    @Test(expected = IllegalStateException.class)
    public void testValues_iterator_after_dispose() {
        map.set(newKey(), newValue());
        Iterator<Long> iterator = map.values().iterator();
        map.dispose();
        iterator.next();
    }

    @Test
    public void testMemoryLeak() {
        int keyRange = 100;

        for (int i = 0; i < 100000; i++) {
            int k = random.nextInt(8);
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

                case 7:
                    _delete_(keyRange);
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
        map.set(newKey(keyRange), newValue());
    }

    private void _putIfAbsent_(int keyRange) {
        map.putIfAbsent(newKey(keyRange), newValue());
    }

    private void _replace_(int keyRange) {
        map.replace(newKey(keyRange), newValue());
    }

    private void _replaceIfSame_(int keyRange) {
        Long key = newKey(keyRange);
        Long value = newValue();
        Long old = map.get(key);
        if (old != MISSING_VALUE) {
            map.replace(key, old, value);
        }
    }

    private void _remove_(int keyRange) {
        map.remove(newKey(keyRange));
    }

    private void _removeIfPresent_(int keyRange) {
        Long key = newKey(keyRange);
        Long old = map.get(key);
        if (old != MISSING_VALUE) {
            map.remove(key, old);
        }
    }

    private void _delete_(int keyRange) {
        map.delete(newKey(keyRange));
    }

    @Test
    public void testDestroyMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

    @Test
    public void testKeyIterMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        SlottableIterator<Long> iter = map.keyIterator();
        while (iter.hasNext()) {
            iter.nextSlot();
            iter.remove();
        }
        assertEquals(0, map.size());

        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

    @Test
    public void testValueIterMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        SlottableIterator<Long> iter = map.valueIterator();
        while (iter.hasNext()) {
            iter.nextSlot();
            iter.remove();
        }
        assertEquals(0, map.size());

        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

    @Test
    public void testEntryIterMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        SlottableIterator<Entry<Long, Long>> iter = map.entryIterator();
        while (iter.hasNext()) {
            iter.nextSlot();
            iter.remove();
        }
        assertEquals(0, map.size());

        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNativeMemory());
    }

    @Test
    public void testMemoryLeak_whenCapacityExpandFails() {
        while (true) {
            // key is on-heap
            Long key = newKey();
            Long value = newValue();
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
