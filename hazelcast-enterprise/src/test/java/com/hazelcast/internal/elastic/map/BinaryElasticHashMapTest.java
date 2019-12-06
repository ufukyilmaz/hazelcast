package com.hazelcast.internal.elastic.map;

import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.internal.elastic.SlottableIterator;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.util.hashslot.impl.CapacityUtil;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class BinaryElasticHashMapTest {

    @Parameters(name = "memoryAllocatorType:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {MemoryAllocatorType.STANDARD},
                {MemoryAllocatorType.POOLED},
        });
    }

    @Parameter
    public MemoryAllocatorType memoryAllocatorType;

    private final Random random = new Random();

    private HazelcastMemoryManager memoryManager;
    private EnterpriseSerializationService serializationService;
    private BinaryElasticHashMap<NativeMemoryData> map;

    @Before
    public void setUp() {
        MemorySize memorySize = new MemorySize(32, MemoryUnit.MEGABYTES);
        if (memoryAllocatorType == MemoryAllocatorType.STANDARD) {
            memoryManager = new StandardMemoryManager(memorySize);
        } else {
            memoryManager = new PoolingMemoryManager(memorySize);
        }

        serializationService = new EnterpriseSerializationServiceBuilder()
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(memoryManager)
                .build();

        NativeMemoryDataAccessor accessor = new NativeMemoryDataAccessor(serializationService);
        NativeBehmSlotAccessorFactory behmSlotAccessorFactory = new NativeBehmSlotAccessorFactory();
        map = new BinaryElasticHashMap<NativeMemoryData>(serializationService, behmSlotAccessorFactory, accessor,
                memoryManager);
    }

    @After
    public void tearDown() {
        if (map.capacity() > 0) {
            map.clear();
        }
        map.dispose();
        serializationService.dispose();
        memoryManager.dispose();
    }

    @Test
    public void testPut() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertNull(map.put(key, value));

        NativeMemoryData newValue = newValue();
        NativeMemoryData oldValue = map.put(key, newValue);
        assertEquals(value, oldValue);
    }

    @Test
    public void testSet() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertTrue(map.set(key, value));

        NativeMemoryData newValue = newValue();
        assertFalse(map.set(key, newValue));
    }

    @Test
    public void testGet() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData currentValue = map.get(key);
        assertEquals(value, currentValue);
    }

    @Test
    public void testGetIfSameKey() {
        Data key = serializationService.toData(random.nextLong(), DataType.NATIVE);
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData currentValue = map.getIfSameKey(key);
        assertEquals(value, currentValue);
    }

    @Test
    public void testPutIfAbsent_success() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertNull(map.putIfAbsent(key, value));
    }

    @Test
    public void testPutIfAbsent_fail() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData newValue = newValue();
        assertEquals(value, map.putIfAbsent(key, newValue));
    }

    @Test
    public void testPutAll() {
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
    public void testReplace() {
        Data key = newKey();
        NativeMemoryData value = newValue();

        assertNull(map.replace(key, value));

        map.set(key, value);

        NativeMemoryData newValue = newValue();
        assertEquals(value, map.replace(key, newValue));
    }

    @Test
    public void testReplace_if_same_success() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData newValue = newValue();
        assertTrue(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_not_exist() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        NativeMemoryData newValue = newValue();
        assertFalse(map.replace(key, value, newValue));
    }

    @Test
    public void testReplace_if_same_fail() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData wrongValue = newValueNotEqualTo(value);
        NativeMemoryData newValue = newValue();
        assertFalse(map.replace(key, wrongValue, newValue));
    }

    @Test
    public void testRemove() {
        Data key = newKey();
        assertNull(map.remove(key));

        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData oldValue = map.remove(key);
        assertEquals(value, oldValue);
    }

    @Test
    public void testDelete() {
        Data key = newKey();
        assertFalse(map.delete(key));

        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.delete(key));
    }

    @Test
    public void testRemove_if_same_success() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_not_exist() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        assertFalse(map.remove(key, value));
    }

    @Test
    public void testRemove_if_same_fail() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        NativeMemoryData wrongValue = newValueNotEqualTo(value);
        assertFalse(map.remove(key, wrongValue));
    }

    @Test
    public void testContainsKey_fail() {
        Data key = newKey();
        assertFalse(map.containsKey(key));
    }

    @Test
    public void testContainsKey_success() {
        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_fail() {
        NativeMemoryData value = newValue();
        assertFalse(map.containsValue(value));
    }

    @Test
    public void testContainsValue_success() {
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
    public void testKeySet() {
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
    public void testKeySet_iterator() {
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

    @Test(expected = ConcurrentModificationException.class)
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testKeySet_iterator_throws_CME() {
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            map.set(key, newValue());
        }

        Iterator<Data> iter = map.keySet().iterator();
        while (iter.hasNext()) {
            Data key = iter.next();
            map.remove(key);
        }
    }

    @Test
    public void testValues() {
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
    public void testValues_iterator() {
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

        Iterator<NativeMemoryData> iter = values.iterator();
        while (iter.hasNext()) {
            NativeMemoryData value = iter.next();
            assertNotNull(value);
            assertTrue(expected.contains(value));

            iter.remove();
        }

        assertTrue(map.isEmpty());
    }

    @Test(expected = ConcurrentModificationException.class)
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testValues_iterator_throws_CME() {
        Data key1 = serializationService.toData(1);
        map.put(key1, newValue());

        Data key2 = serializationService.toData(2);
        map.put(key2, newValue());

        Iterator<NativeMemoryData> iterator = map.values().iterator();
        while (iterator.hasNext()) {
            iterator.next();
            map.remove(key1);
        }
    }

    @Test
    public void testEntrySet() {
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

    @Test(expected = ConcurrentModificationException.class)
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testEntrySet_iterator_throws_CME() {
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            map.set(key, newValue());
        }

        Iterator<Entry<Data, NativeMemoryData>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Data, NativeMemoryData> next = iter.next();
            map.remove(next.getKey());
        }
    }

    @Test(expected = ConcurrentModificationException.class)
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testEntrySet_iterator_throws_CME_onClear() {
        for (int i = 0; i < 100; i++) {
            Data key = serializationService.toData(i);
            map.set(key, newValue());
        }

        Iterator<Entry<Data, NativeMemoryData>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            iter.next();
            map.clear();
        }
    }


    @Test
    public void testEntrySet_iterator() {
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
    public void testSize() {
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
    public void testClear() {
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
    public void testIsEmpty() {
        assertTrue(map.isEmpty());

        Data key = newKey();
        NativeMemoryData value = newValue();
        map.set(key, value);

        assertFalse(map.isEmpty());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testGet_after_dispose() {
        map.dispose();
        map.get(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testPut_after_dispose() {
        map.dispose();
        map.put(newKey(), newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testRemove_after_dispose() {
        map.dispose();
        map.remove(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testReplace_after_dispose() {
        map.dispose();
        map.replace(newKey(), newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testContainsKey_after_dispose() {
        map.dispose();
        map.containsKey(newKey());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testContainsValue_after_dispose() {
        map.dispose();
        map.containsValue(newValue());
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testKeySet_after_dispose() {
        map.dispose();
        map.keySet();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testEntrySet_after_dispose() {
        map.dispose();
        map.entrySet();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testValues_after_dispose() {
        map.dispose();
        map.values();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testKeySet_iterator_after_dispose() {
        map.set(newKey(), newValue());
        Iterator<Data> iterator = map.keySet().iterator();
        map.dispose();
        iterator.next();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testEntrySet_iterator_after_dispose() {
        map.set(newKey(), newValue());
        Iterator<Map.Entry<Data, NativeMemoryData>> iterator = map.entrySet().iterator();
        map.dispose();
        iterator.next();
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void testValues_iterator_after_dispose() {
        map.set(newKey(), newValue());
        Iterator<NativeMemoryData> iterator = map.values().iterator();
        map.dispose();
        iterator.next();
    }

    @Test
    public void testMemoryLeak() {
        int keyRange = 100;

        for (int i = 0; i < 100000; i++) {
            int methodId = random.nextInt(8);
            switch (methodId) {
                case 0:
                    putInternal(keyRange);
                    break;
                case 1:
                    setInternal(keyRange);
                    break;
                case 2:
                    putIfAbsentInternal(keyRange);
                    break;
                case 3:
                    replaceInternal(keyRange);
                    break;
                case 4:
                    replaceIfSameInternal(keyRange);
                    break;
                case 5:
                    removeInternal(keyRange);
                    break;
                case 6:
                    removeIfPresentInternal(keyRange);
                    break;
                case 7:
                    deleteInternal(keyRange);
                    break;
            }
        }

        map.clear();
        map.dispose();
        MemoryStats memoryStats = memoryManager.getMemoryStats();
        assertEquals(memoryStats.toString(), 0, memoryStats.getUsedNative());
    }

    private void putInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData value = newValue();
        NativeMemoryData old = map.put(key, value);
        if (old != null) {
            serializationService.disposeData(old);
        }
    }

    private void setInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData value = newValue();
        map.set(key, value);
    }

    private void putIfAbsentInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData value = newValue();
        NativeMemoryData old = map.putIfAbsent(key, value);
        if (old != null) {
            serializationService.disposeData(value);
        }
    }

    private void replaceInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData value = newValue();
        NativeMemoryData old = map.replace(key, value);
        serializationService.disposeData(key);
        if (old != null) {
            serializationService.disposeData(old);
        } else {
            serializationService.disposeData(value);
        }
    }

    private void replaceIfSameInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData value = newValue();
        NativeMemoryData old = map.get(key);
        if (old != null) {
            map.replace(key, old, value);
        } else {
            serializationService.disposeData(value);
        }
        serializationService.disposeData(key);
    }

    private void removeInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData old = map.remove(key);
        serializationService.disposeData(key);
        if (old != null) {
            serializationService.disposeData(old);
        }
    }

    private void removeIfPresentInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
        NativeMemoryData old = map.get(key);
        if (old != null) {
            map.remove(key, old);
            serializationService.disposeData(key);
            serializationService.disposeData(old);
        }
    }

    private void deleteInternal(int keyRange) {
        Data key = newKey(random.nextInt(keyRange));
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
        assumeTrue(memoryAllocatorType == MemoryAllocatorType.STANDARD);

        byte[] bytes = new byte[8];

        while (true) {
            // key is on-heap
            Data key = newKey();
            // the value is off-heap
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
    public void testPut_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        assertNull(map.put(key, value));
    }

    @Test
    public void testPut_put_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        assertNull(map.put(key, value));
        assertNull(map.put(key, value));
    }

    @Test
    public void testGet_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.get(key));
    }

    @Test
    public void testContainsKey_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testPutIfAbsent_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.putIfAbsent(key, newValue()));
    }

    @Test
    public void testSet_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.set(key, value);
    }

    @Test
    public void testSet_setNullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        assertTrue(map.set(key, value));
        assertTrue(map.set(key, value));
    }

    @Test
    public void testRemove_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.remove(key));
    }

    @Test
    public void testDelete_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertFalse(map.delete(key));
    }

    @Test
    public void testRemoveIfSame_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.remove(key, value));
    }

    @Test
    public void testReplace_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertNull(map.replace(key, newValue()));
    }

    @Test
    public void testReplaceIfSame_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertTrue(map.replace(key, value, newValue()));
    }

    @Test
    public void testClear_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        map.clear();
    }

    @Test
    public void testIsEmpty_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertFalse(map.isEmpty());
    }

    @Test
    public void testSize_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        assertEquals(1, map.size());
    }

    @Test
    public void testDispose_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);
        map.dispose();
    }

    @Test
    public void testKeySet_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Set<Data> keySet = map.keySet();
        assertEquals(1, keySet.size());
    }

    @Test
    public void testKeySetIterator_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Iterator<Data> iter = map.keySet().iterator();
        assertTrue(iter.hasNext());
        assertEquals(key, iter.next());
    }

    @Test
    public void testValues_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Collection<NativeMemoryData> values = map.values();
        assertEquals(1, values.size());
    }

    @Test
    public void testValuesIterator_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Iterator<NativeMemoryData> iter = map.values().iterator();
        assertTrue(iter.hasNext());
        assertNull(iter.next());
    }

    @Test
    public void testEntrySet_nullValue() {
        Data key = newKey();
        NativeMemoryData value = new NativeMemoryData();
        map.put(key, value);

        Set<Map.Entry<Data, NativeMemoryData>> entries = map.entrySet();
        assertEquals(1, entries.size());
    }

    @Test
    public void testEntrySetIterator_nullValue() {
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
    public void testGetNativeKeyAddress() {
        Data key = newKey();
        map.put(key, newValue());

        long nativeKeyAddress = map.getNativeKeyAddress(key);
        assertNotEquals(NULL_ADDRESS, nativeKeyAddress);

        NativeMemoryData nativeKey = new NativeMemoryData().reset(nativeKeyAddress);
        assertEquals(key, nativeKey);
    }

    @Test
    public void testNull_getNativeKeyAddress() {
        Data key = newKey();
        long nativeKeyAddress = map.getNativeKeyAddress(key);
        assertEquals(NULL_ADDRESS, nativeKeyAddress);
    }

    @Test
    public void testCapacity() {
        assertEquals(CapacityUtil.DEFAULT_CAPACITY, map.capacity());
    }

    @Test
    public void testCapacity_afterDispose() {
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
        byte[] bytes = new byte[random.nextInt(1000) + 100];
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
