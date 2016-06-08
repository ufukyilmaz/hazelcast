package com.hazelcast.elastic.map;

import com.hazelcast.internal.util.hashslot.impl.CapacityUtil;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
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

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BinaryElasticHashMapTest {

    private final Random random = new Random();
    private HazelcastMemoryManager memoryManager;
    private EnterpriseSerializationService serializationService;
    private BinaryElasticHashMap<NativeMemoryData> map;

    @Before
    public void setUp() throws Exception {
        memoryManager = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));

        serializationService
                = new EnterpriseSerializationServiceBuilder()
                .setAllowUnsafe(true).setUseNativeByteOrder(true)
                .setMemoryManager(memoryManager)
                .build();

        map = new BinaryElasticHashMap<NativeMemoryData>(serializationService,
                new NativeMemoryDataAccessor(serializationService), memoryManager);
    }

    @After
    public void tearDown() throws Exception {
        if (map.capacity() > 0) {
            map.clear();
        }
        map.dispose();
        serializationService.dispose();
        memoryManager.dispose();
    }

    @Test
    public void testPut() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertNull(map.put(key, value));

        NativeMemoryData newValue = newValue();
        NativeMemoryData oldValue = map.put(key, newValue);
        assertEquals(value, oldValue);
    }

    @Test
    public void testSet() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertTrue(map.set(key, value));

        NativeMemoryData newValue = newValue();
        assertFalse(map.set(key, newValue));
    }

    @Test
    public void testGet() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData currentValue = map.get(key);
        assertEquals(value, currentValue);
    }

    @Test
    public void testGetIfSameKey() throws Exception {
        Data key = serializationService.toData(random.nextLong(), DataType.NATIVE);
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData currentValue = map.getIfSameKey(key);
        assertEquals(value, currentValue);
    }

    @Test
    public void testPutIfAbsent_success() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertNull(map.putIfAbsent(key, value));
    }

    @Test
    public void testPutIfAbsent_fail() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData newValue = newValue();
        assertEquals(value, map.putIfAbsent(key, newValue));
    }

    @Test
    public void testPutAll() throws Exception {
        int count = 100;
        Map<Data, NativeMemoryData> entries = new HashMap<Data, NativeMemoryData>(count);

        for (int i = 0; i < count; i++) {
            Data key = newKey();
            NativeMemoryData value = newValue();
            entries.put(key, value);
        }

        map.putAll(entries);
        assertEquals(count, map.size());

        for (Map.Entry<Data, NativeMemoryData> entry : entries.entrySet()) {
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    public void testReplace() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();

        assertNull(map.replace(key, value));

        map.set(key, value);

        NativeMemoryData newValue = newValue();
        assertEquals(value, map.replace(key, newValue));
    }

    @Test
    public void testReplace_if_same_success() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData newValue = newValue();
        assertTrue(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_not_exist() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        NativeMemoryData newValue = newValue();
        assertFalse(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_fail() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData wrongValue = newValueNotEqualTo(value);
        NativeMemoryData newValue = newValue();
        assertFalse(map.replace(key, wrongValue, newValue));
    }

    @Test
    public void testRemove() throws Exception {
        Data key = newKey();
        assertNull(map.remove(key));

        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData oldValue = map.remove(key);
        assertEquals(value, oldValue);
    }

    @Test
    public void testDelete() throws Exception {
        Data key = newKey();
        assertFalse(map.delete(key));

        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.delete(key));
    }

    @Test
    public void testRemove_if_same_success() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_not_exist() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertFalse(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_fail() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData wrongValue = newValueNotEqualTo(value);
        assertFalse(map.remove(key, wrongValue));
    }

    @Test
    public void testContainsKey_fail() throws Exception {
        Data key = newKey();
        assertFalse(map.containsKey(key));
    }

    @Test
    public void testContainsKey_success() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_fail() throws Exception {
        NativeMemoryData value = newValue();
        assertFalse(map.containsValue(value));
    }

    @Test
    public void testContainsValue_success() throws Exception {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.containsValue(value));
    }

    @Test
    public void testPut_withTheSameValue() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.put(key, value);

        NativeMemoryData oldValue = map.put(key, value);
        assertEquals(value, oldValue);
    }

    @Test
    public void testSet_withTheSameValue() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        NativeMemoryData expected = new NativeMemoryData(value.address(), value.size());
        map.set(key, value);

        map.set(key, value);
        assertEquals(expected, value);
    }

    @Test
    public void testKeySet() throws Exception {
        Set<Data> keys = map.keySet();
        assertTrue(keys.isEmpty());

        Set<Data> expected = new HashSet<Data>();
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            map.set(key, newValue());
            expected.add(key);
        }

        keys = map.keySet();
        assertEquals(expected.size(), keys.size());
        assertEquals(expected, keys);
    }

    @Test
    public void testKeySet_iterator() throws Exception {
        Set<Data> keys = map.keySet();
        assertFalse(keys.iterator().hasNext());

        Set<Data> expected = new HashSet<Data>();
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            map.set(key, newValue());
            expected.add(key);
        }

        keys = map.keySet();
        assertEquals(expected.size(), keys.size());

        Iterator<Data> iter = keys.iterator();
        while (iter.hasNext()) {
            Data key = iter.next();
            assertNotNull(key);
            assertTrue(expected.contains(key));

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test
    public void testValues() throws Exception {
        Collection<NativeMemoryData> values = map.values();
        assertTrue(values.isEmpty());

        Set<NativeMemoryData> expected = new HashSet<NativeMemoryData>();
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            NativeMemoryData value = newValue();
            map.set(key, value);
            expected.add(value);
        }

        values = map.values();
        assertEquals(expected.size(), values.size());
        assertEquals(expected, new HashSet<NativeMemoryData>(values));
    }

    @Test
    public void testValues_iterator() throws Exception {
        Collection<NativeMemoryData> values = map.values();
        assertFalse(values.iterator().hasNext());

        Set<NativeMemoryData> expected = new HashSet<NativeMemoryData>();
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            NativeMemoryData value = newValue();
            map.set(key, value);
            expected.add(value);
        }

        values = map.values();
        assertEquals(expected.size(), values.size());

        Iterator<NativeMemoryData> iter = values.iterator();
        while (iter.hasNext()) {
            NativeMemoryData value = iter.next();
            assertNotNull(value);
            assertTrue(expected.contains(value));

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test
    public void testEntrySet() throws Exception {
        Set<Map.Entry<Data, NativeMemoryData>> entries = map.entrySet();
        assertTrue(entries.isEmpty());

        Map<Data, NativeMemoryData> expected = new HashMap<Data, NativeMemoryData>();
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            NativeMemoryData value = newValue();
            map.set(key, value);
            expected.put(key, value);
        }

        entries = map.entrySet();
        assertEquals(expected.size(), entries.size());
        assertEquals(expected.entrySet(), entries);
    }

    @Test
    public void testEntrySet_iterator() throws Exception {
        Set<Map.Entry<Data, NativeMemoryData>> entries = map.entrySet();
        assertFalse(entries.iterator().hasNext());

        Map<Data, NativeMemoryData> expected = new HashMap<Data, NativeMemoryData>();
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            NativeMemoryData value = newValue();
            map.set(key, value);
            expected.put(key, value);
        }

        entries = map.entrySet();
        assertEquals(expected.size(), entries.size());

        Iterator<Map.Entry<Data, NativeMemoryData>> iter = entries.iterator();
        while (iter.hasNext()) {
            Map.Entry<Data, NativeMemoryData> entry = iter.next();
            assertNotNull(entry);
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());

            assertEquals(expected.get(entry.getKey()), entry.getValue());

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(0, map.size());

        int expected = 100;
        for (int i = 0; i < expected; i++) {
            Data key = serializationService.toData(i);
            NativeMemoryData value = newValue();
            map.set(key, value);
        }

        assertEquals(expected, map.size());
    }

    @Test
    public void testClear() throws Exception {
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            NativeMemoryData value = newValue();
            map.set(key, value);
        }

        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertTrue(map.isEmpty());

        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertFalse(map.isEmpty());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testGet_after_dispose() throws Exception {
        map.dispose();
        map.get(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testPut_after_dispose() throws Exception {
        map.dispose();
        map.put(newKey(), newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testRemove_after_dispose() throws Exception {
        map.dispose();
        map.remove(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testReplace_after_dispose() throws Exception {
        map.dispose();
        map.replace(newKey(), newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testContainsKey_after_dispose() throws Exception {
        map.dispose();
        map.containsKey(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testContainsValue_after_dispose() throws Exception {
        map.dispose();
        map.containsValue(newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testKeySet_after_dispose() throws Exception {
        map.dispose();
        map.keySet();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testEntrySet_after_dispose() throws Exception {
        map.dispose();
        map.entrySet();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testValues_after_dispose() throws Exception {
        map.dispose();
        map.values();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testKeySet_iterator_after_dispose() throws Exception {
        map.set(newKey(), newValue());
        Iterator<Data> iterator = map.keySet().iterator();
        map.dispose();
        iterator.next();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testEntrySet_iterator_after_dispose() throws Exception {
        map.set(newKey(), newValue());
        Iterator<Map.Entry<Data, NativeMemoryData>> iterator = map.entrySet().iterator();
        map.dispose();
        iterator.next();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testValues_iterator_after_dispose() throws Exception {
        map.set(newKey(), newValue());
        Iterator<NativeMemoryData> iterator = map.values().iterator();
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
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    private void _put_(int keyRange) {
        Data key;
        NativeMemoryData value;
        NativeMemoryData old;
        key = newKey(random.nextInt(keyRange));
        value = newValue();
        old = map.put(key, value);
        if (old != null) {
            serializationService.disposeData(old);
        }
    }

    private void _set_(int keyRange) {
        Data key;
        NativeMemoryData value;
        key = newKey(random.nextInt(keyRange));
        value = newValue();
        map.set(key, value);
    }

    private void _putIfAbsent_(int keyRange) {
        Data key;
        NativeMemoryData value;
        NativeMemoryData old;
        key = newKey(random.nextInt(keyRange));
        value = newValue();
        old = map.putIfAbsent(key, value);
        if (old != null) {
            serializationService.disposeData(value);
        }
    }

    private void _replace_(int keyRange) {
        Data key;
        NativeMemoryData value;
        NativeMemoryData old;
        key = newKey(random.nextInt(keyRange));
        value = newValue();
        old = map.replace(key, value);
        serializationService.disposeData(key);
        if (old != null) {
            serializationService.disposeData(old);
        } else {
            serializationService.disposeData(value);
        }
    }

    private void _replaceIfSame_(int keyRange) {
        Data key;
        NativeMemoryData value;
        NativeMemoryData old;
        key = newKey(random.nextInt(keyRange));
        value = newValue();
        old = map.get(key);
        if (old != null) {
            map.replace(key, old, value);
        } else {
            serializationService.disposeData(value);
        }
        serializationService.disposeData(key);
    }

    private void _remove_(int keyRange) {
        Data key;
        NativeMemoryData old;
        key = newKey(random.nextInt(keyRange));
        old = map.remove(key);
        serializationService.disposeData(key);
        if (old != null) {
            serializationService.disposeData(old);
        }
    }

    private void _removeIfPresent_(int keyRange) {
        Data key;
        NativeMemoryData old;
        key = newKey(random.nextInt(keyRange));
        old = map.get(key);
        if (old != null) {
            map.remove(key, old);
            serializationService.disposeData(key);
            serializationService.disposeData(old);
        }
    }

    private void _delete_(int keyRange) {
        Data key;
        key = newKey(random.nextInt(keyRange));
        map.delete(key);
        serializationService.disposeData(key);
    }

    @Test
    public void testDestroyMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    @Test
    public void testKeyIterMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        BinaryElasticHashMap<NativeMemoryData>.KeyIter iter = map.keyIter(0);
        while (iter.hasNext()) {
            iter.nextSlot();
            iter.remove();
        }
        assertEquals(0, map.size());

        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    @Test
    public void testValueIterMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        BinaryElasticHashMap<NativeMemoryData>.ValueIter iter = map.new ValueIter();
        while (iter.hasNext()) {
            iter.nextSlot();
            iter.remove();
        }
        assertEquals(0, map.size());

        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    @Test
    public void testEntryIterMemoryLeak() {
        for (int i = 0; i < 100; i++) {
            map.set(newKey(), newValue());
        }

        SlottableIterator<Entry<Data, NativeMemoryData>> iter = map.entryIter(0);
        while (iter.hasNext()) {
            iter.nextSlot();
            iter.remove();
        }
        assertEquals(0, map.size());

        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    @Test
    public void testMemoryLeak_whenCapacityExpandFails() {
        byte[] bytes = new byte[8];

        while (true) {
            // key is on-heap
            Data key = newKey();
            NativeMemoryData value = newValue(bytes);
            try {
                map.put(key, value);
            } catch (NativeOutOfMemoryError e) {
                // dispose native value
                serializationService.disposeData(value);
                break;
            }
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    @Test
    public void test_put_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        assertNull(map.put(key, value));
    }

    @Test
    public void test_put_put_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        assertNull(map.put(key, value));
        assertNull(map.put(key, value));
    }

    @Test
    public void test_get_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.get(key));
    }

    @Test
    public void test_containsKey_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.containsKey(key));
    }

    @Test
    public void test_containsValue_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void test_putIfAbsent_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.putIfAbsent(key, newValue()));
    }

    @Test
    public void test_set_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.set(key, value);
    }

    @Test
    public void test_set_set_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        assertTrue(map.set(key, value));
        assertTrue(map.set(key, value));
    }

    @Test
    public void test_remove_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.remove(key));
    }

    @Test
    public void test_delete_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertFalse(map.delete(key));
    }

    @Test
    public void test_removeIfSame_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.remove(key, value));
    }

    @Test
    public void test_replace_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.replace(key, newValue()));
    }

    @Test
    public void test_replaceIfSame_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.replace(key, value, newValue()));
    }

    @Test
    public void test_clear_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        map.clear();
    }

    @Test
    public void test_isEmpty_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertFalse(map.isEmpty());
    }

    @Test
    public void test_size_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertEquals(1, map.size());
    }

    @Test
    public void test_dispose_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        map.dispose();
    }

    @Test
    public void test_keySet_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Set<Data> keySet = map.keySet();
        assertEquals(1, keySet.size());
    }

    @Test
    public void test_keySetIterator_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Iterator<Data> iter = map.keySet().iterator();
        assertTrue(iter.hasNext());
        assertEquals(key, iter.next());
    }

    @Test
    public void test_values_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Collection<NativeMemoryData> values = map.values();
        assertEquals(1, values.size());
    }

    @Test
    public void test_valuesIterator_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Iterator<NativeMemoryData> iter = map.values().iterator();
        assertTrue(iter.hasNext());
        assertNull(iter.next());
    }

    @Test
    public void test_entrySet_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Set<Map.Entry<Data, NativeMemoryData>> entries = map.entrySet();
        assertEquals(1, entries.size());
    }

    @Test
    public void test_entrySetIterator_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Iterator<Map.Entry<Data, NativeMemoryData>> iter = map.entrySet().iterator();
        assertTrue(iter.hasNext());
        Map.Entry<Data, NativeMemoryData> entry = iter.next();
        assertEquals(key, entry.getKey());
        assertNull(entry.getValue());
    }

    @Test
    public void test_getNativeKeyAddress() {
        Data key = newKey();
        map.put(key, newValue());

        long nativeKeyAddress = map.getNativeKeyAddress(key);
        assertNotEquals(NULL_ADDRESS, nativeKeyAddress);

        NativeMemoryData nativeKey = new NativeMemoryData().reset(nativeKeyAddress);
        assertEquals(key, nativeKey);
    }

    @Test
    public void test_null_getNativeKeyAddress() {
        Data key = newKey();
        long nativeKeyAddress = map.getNativeKeyAddress(key);
        assertEquals(NULL_ADDRESS, nativeKeyAddress);
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
    public void testRandomKeyIterator_removeWillClearAllEntriesEventually() {
        int entryCount = 1000;
        for (int i = 0; i < entryCount; i++) {
            Data key = newKey();
            NativeMemoryData value = newValue();
            map.set(key, value);
        }

        SlottableIterator<Data> dataSlottableIterator = map.newRandomEvictionKeyIterator();
        while (dataSlottableIterator.hasNext()) {
            dataSlottableIterator.next();
            dataSlottableIterator.remove();
        }

        assertEquals(0, map.size());
    }

    @Test
    public void testRandomValueIterator_removeWillClearAllEntriesEventually() {
        int entryCount = 1000;
        for (int i = 0; i < entryCount; i++) {
            Data key = newKey();
            NativeMemoryData value = newValue();
            map.set(key, value);
        }

        SlottableIterator<NativeMemoryData> dataSlottableIterator = map.newRandomEvictionValueIterator();
        while (dataSlottableIterator.hasNext()) {
            dataSlottableIterator.next();
            dataSlottableIterator.remove();
        }

        assertEquals(0, map.size());
    }

    private Data newKey() {
        return serializationService.toData(random.nextLong());
    }

    private Data newKey(long k) {
        return serializationService.toData(k);
    }

    private NativeMemoryData newValue() {
        byte[] bytes = new byte[random.nextInt(1000) + 1];
        random.nextBytes(bytes);
        return serializationService.toData(bytes, DataType.NATIVE);
    }

    private NativeMemoryData newValue(byte[] bytes) {
        return serializationService.toData(bytes, DataType.NATIVE);
    }

    private NativeMemoryData newValueNotEqualTo(NativeMemoryData value) {
        NativeMemoryData v = null;
        do {
            if (v != null) {
                serializationService.disposeData(v);
            }
            v = newValue();
        } while (value.equals(v));
        return v;
    }
}
