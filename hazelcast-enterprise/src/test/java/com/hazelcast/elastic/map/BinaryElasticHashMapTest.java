package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.test.HazelcastSerialClassRunner;
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
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BinaryElasticHashMapTest {

    private final Random random = new Random();
    private MemoryManager memoryManager;
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
        map.destroy();
        serializationService.destroy();
        memoryManager.destroy();
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
    public void testGet_after_destroy() throws Exception {
        map.destroy();
        map.get(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testPut_after_destroy() throws Exception {
        map.destroy();
        map.put(newKey(), newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testRemove_after_destroy() throws Exception {
        map.destroy();
        map.remove(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testReplace_after_destroy() throws Exception {
        map.destroy();
        map.replace(newKey(), newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testContainsKey_after_destroy() throws Exception {
        map.destroy();
        map.containsKey(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testContainsValue_after_destroy() throws Exception {
        map.destroy();
        map.containsValue(newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testKeySet_after_destroy() throws Exception {
        map.destroy();
        map.keySet();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testEntrySet_after_destroy() throws Exception {
        map.destroy();
        map.entrySet();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testValues_after_destroy() throws Exception {
        map.destroy();
        map.values();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testKeySet_iterator_after_destroy() throws Exception {
        map.set(newKey(), newValue());
        Iterator<Data> iterator = map.keySet().iterator();
        map.destroy();
        iterator.next();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testEntrySet_iterator_after_destroy() throws Exception {
        map.set(newKey(), newValue());
        Iterator<Map.Entry<Data, NativeMemoryData>> iterator = map.entrySet().iterator();
        map.destroy();
        iterator.next();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testValues_iterator_after_destroy() throws Exception {
        map.set(newKey(), newValue());
        Iterator<NativeMemoryData> iterator = map.values().iterator();
        map.destroy();
        iterator.next();
    }

    private Data newKey() {
        return serializationService.toData(random.nextLong());
    }

    private NativeMemoryData newValue() {
        byte[] bytes = new byte[random.nextInt(1000) + 1];
        random.nextBytes(bytes);
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
