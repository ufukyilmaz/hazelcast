package com.hazelcast.internal.elastic.tree;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.DataType.HEAP;
import static com.hazelcast.internal.serialization.DataType.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BinaryElasticNestedTreeMapTest {

    private HazelcastMemoryManager malloc;
    private NativeBinaryElasticNestedTreeMap<Map.Entry>  map;
    private EnterpriseSerializationService ess;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(200, MemoryUnit.MEGABYTES));
        this.ess = getSerializationService();
        this.map = new NativeBinaryElasticNestedTreeMap(ess, malloc, new ComparableComparator(ess));
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(100, MemoryUnit.MEGABYTES);
        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
                .setSize(memorySize).setEnabled(true)
                .setMinBlockSize(16).setPageSize(1 << 20);
    }

    private EnterpriseSerializationService getSerializationService() {
        NativeMemoryConfig memoryConfig = getMemoryConfig();
        int blockSize = memoryConfig.getMinBlockSize();
        int pageSize = memoryConfig.getPageSize();
        float metadataSpace = memoryConfig.getMetadataSpacePercentage();

        HazelcastMemoryManager memoryManager =
                new PoolingMemoryManager(memoryConfig.getSize(), blockSize, pageSize, metadataSpace);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(malloc)
                .build();
    }

    @Test
    public void put_get_clear_multipleTimes() throws IOException {
        Data segmentKey = heapData(1);
        List<NativeMemoryData> localAllocs = new ArrayList<NativeMemoryData>();

        for (int i = 1; i <= 10000; i++) {
            NativeMemoryData key = nativeData(100);
            NativeMemoryData value = nativeData(200);

            localAllocs.add(key);
            localAllocs.add(value);

            map.put(segmentKey, key, value);

            NativeMemoryData valueGot = map.get(segmentKey, key);
            assertEquals("iteration:" + i, value, valueGot);

            map.clear();
            assertEquals(0, map.size());
        }

        map.dispose();
        for (NativeMemoryData data : localAllocs) {
            ess.disposeData(data, malloc);
        }

        assertEquals(0, malloc.getMemoryStats().getUsedNative());
    }

    @Test
    public void put_get_remove_nullValue_multipleTimes() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = ess.toData(100, NATIVE);
        NativeMemoryData value = null;

        for (int i = 1; i <= 10000; i++) {
            map.put(segmentKey, key, value);

            Data valueGot = map.get(segmentKey, key);
            assertEquals(value, valueGot);

            map.remove(segmentKey, key);

            assertNull(map.get(segmentKey, key));
            assertEquals(0, map.size());
        }
    }

    @Test
    public void get_value() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = ess.toData(100, NATIVE);
        NativeMemoryData value = ess.toData(200, NATIVE);

        map.put(segmentKey, key, value);
        Data valueGot = map.get(segmentKey, key);

        assertEquals(value, valueGot);
    }

    @Test
    public void get_nullValue() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = ess.toData(100, NATIVE);
        NativeMemoryData value = null;

        map.put(segmentKey, key, value);
        Data valueGot = map.get(segmentKey, (NativeMemoryData) ess.toData(101, NATIVE));

        assertNull(valueGot);
    }

    @Test
    public void get_notExistingSegment() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = ess.toData(100, NATIVE);

        Data valueGot = map.get(segmentKey, key);

        assertEquals(valueGot, map.get(segmentKey, key));
    }

    @Test
    public void get_notExistingSegment_otherSegmentsExists() throws IOException {
        map.put(heapData(2), nativeData(100), nativeData(200));
        map.put(heapData(3), nativeData(100), nativeData(600));

        Data segmentKey = heapData(1);
        NativeMemoryData key = nativeData(100);
        Data valueGot = map.get(segmentKey, key);

        assertEquals(valueGot, map.get(segmentKey, key));
    }

    @Test
    public void get_notExistingSegment_otherSegmentsExists_string() throws IOException {
        map.put(ess.toData("0", HEAP), nativeData(100), nativeData(200));
        map.put(ess.toData("1", HEAP), nativeData(100), nativeData(600));

        Data segmentKey = heapData("2");
        Set<Map.Entry> entries = map.get(segmentKey);

        assertEquals(0, entries.size());
    }

    @Test
    public void get_notExistingKey() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = nativeData(100);
        NativeMemoryData value = nativeData(200);

        map.put(segmentKey, key, value);
        Data valueGot = map.get(segmentKey, nativeData(101));

        assertNull(valueGot);
    }

    @Test
    public void get_segment() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN
        Set<Map.Entry> records = map.get(heapData(1));

        // THEN
        assertEquals(300, map.size());
        assertEquals(100, records.size());
        assertSegmentContent(records, 101, 200);
    }

    @Test
    public void get_segment_notExistingSegmentKey() throws IOException {
        // GIVEN
        putToSegment(1, 101, 200);

        // WHEN
        Set<Map.Entry> records = map.get(heapData(4));

        // THEN
        assertEquals(0, records.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void get_segment_nullSegmentKey() throws IOException {
        // GIVEN
        putToSegment(1, 101, 200);

        // WHEN
        Set<Map.Entry> records = map.get(new HeapData());
    }

    @Test
    public void getSet_nullValue() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = nativeData(100);
        NativeMemoryData value = null;

        map.put(segmentKey, key, value);
        Data valueGot = map.get(segmentKey, key);

        assertNull(valueGot);
    }

    @Test
    public void getSet_emptyValue() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = nativeData(100);
        NativeMemoryData value = new NativeMemoryData();

        map.put(segmentKey, key, value);
        Data valueGot = map.get(segmentKey, key);

        assertNull(valueGot);
    }

    @Test
    public void sizeAndClearOnEmpty() throws IOException {
        assertEquals(0, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void put_get_remove_multipleTimes() throws IOException {
        Data segmentKey = heapData(1);
        NativeMemoryData key = ess.toData(100, NATIVE);
        NativeMemoryData value = ess.toData(200, NATIVE);

        for (int i = 1; i <= 10000; i++) {
            map.put(segmentKey, key, value);

            Data valueGot = map.get(segmentKey, key);
            assertEquals(value, valueGot);

            map.remove(segmentKey, key);

            assertEquals(null, map.get(segmentKey, key));
            assertEquals(0, map.size());
        }
    }

    @Test
    public void subMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN (exclusive, inclusive)
        Set<Map.Entry> records = map.subMap(heapData(2), false, heapData(3), true);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (inclusive, exclusive)
        records = map.subMap(heapData(2), true, heapData(3), false);
        assertSegmentContent(records, 201, 300);

        // WHEN & THEN (inclusive, inclusive)
        records = map.subMap(heapData(1), true, heapData(3), true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (exclusive, exclusive)
        records = map.subMap(heapData(1), false, heapData(3), false);
        assertSegmentContent(records, 201, 300);

        // WHEN & THEN (exclusive, exclusive) -> empty
        records = map.subMap(heapData(1), false, heapData(1), false);
        assertEquals(0, records.size());

        // WHEN & THEN (inclusive, not found)
        records = map.subMap(heapData(2), true, heapData(5), false);
        assertSegmentContent(records, 201, 400);

        // WHEN & THEN (exclusive last, not found)
        records = map.subMap(heapData(3), false, heapData(5), false);
        assertEquals(0, records.size());

        // WHEN & THEN (not found, ...)
        records = map.subMap(heapData(140), true, heapData(5), false);
        assertEquals(0, records.size());

        // WHEN & THEN (both null & inclusive)
        records = map.subMap(null, true, null, true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (both null & exclusive)
        records = map.subMap(null, false, null, false);
        assertSegmentContent(records, 101, 400);
    }

    @Test
    public void subMap_cornerCases() throws IOException {
        // GIVEN
        putToSegment(1, 101, 200);
        putToSegment(3, 201, 300);
        putToSegment(5, 301, 400);

        // WHEN & THEN
        Set<Map.Entry> records = map.subMap(heapData(2), false, heapData(3), true);
        assertSegmentContent(records, 201, 300);

        records = map.subMap(heapData(2), true, heapData(3), true);
        assertSegmentContent(records, 201, 300);

        records = map.subMap(heapData(2), true, heapData(3), false);
        assertEquals(0, records.size());

        records = map.subMap(heapData(2), true, heapData(4), true);
        assertSegmentContent(records, 201, 300);

        records = map.subMap(heapData(2), false, heapData(4), false);
        assertSegmentContent(records, 201, 300);

        records = map.subMap(heapData(4), false, ess.toData(6, HEAP), false);
        assertSegmentContent(records, 301, 400);

        records = map.subMap(ess.toData(0, HEAP), false, heapData(2), false);
        assertSegmentContent(records, 101, 200);

        records = map.subMap(ess.toData(-1, HEAP), false, heapData(138), false);
        assertSegmentContent(records, 101, 400);
    }

    @Test
    public void headMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN (exclusive)
        Set<Map.Entry> records = map.headMap(heapData(2), false);
        assertSegmentContent(records, 101, 200);

        // WHEN & THEN (inclusive)
        records = map.headMap(heapData(2), true);
        assertSegmentContent(records, 101, 300);

        // WHEN & THEN (inclusive, edge)
        records = map.headMap(heapData(1), true);
        assertSegmentContent(records, 101, 200);

        // WHEN & THEN (exclusive, edge)
        records = map.headMap(heapData(1), false);
        assertEquals(0, records.size());

        // WHEN & THEN (exclusive, non-existing)
        records = map.headMap(heapData(123), false);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (inclusive, non-existing)
        records = map.headMap(heapData(123), true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (inclusive, null)
        records = map.headMap(null, true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (exclusive, null)
        records = map.headMap(null, false);
        assertSegmentContent(records, 101, 400);
    }

    @Test
    public void headMap_notExisting_butInRange() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(4, 301, 400);

        // WHEN & THEN (in-between, exclusive)
        Set<Map.Entry> records = map.headMap(heapData(3), false);
        assertSegmentContent(records, 101, 300);

        // WHEN & THEN (in-between, inclusive)
        records = map.headMap(heapData(3), true);
        assertSegmentContent(records, 101, 300);

        // WHEN & THEN (after, exclusive)
        records = map.headMap(heapData(5), true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (after, inclusive)
        records = map.headMap(heapData(5), false);
        assertSegmentContent(records, 101, 400);
    }


    @Test
    public void tailMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN (exclusive)
        Set<Map.Entry> records = map.tailMap(heapData(2), false);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (inclusive)
        records = map.tailMap(heapData(2), true);
        assertSegmentContent(records, 201, 400);

        // WHEN & THEN (inclusive, edge)
        records = map.tailMap(heapData(3), true);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (exclusive, edge)
        records = map.tailMap(heapData(3), false);
        assertEquals(0, records.size());

        // WHEN & THEN (exclusive, non-existing)
        records = map.tailMap(heapData(123), false);
        assertEquals(0, records.size());

        // WHEN & THEN (inclusive, non-existing)
        records = map.tailMap(heapData(123), true);
        assertEquals(0, records.size());

        // WHEN & THEN (exclusive, null)
        records = map.tailMap(null, false);
        assertEquals(0, records.size());

        // WHEN & THEN (inclusive, null)
        records = map.tailMap(null, true);
        assertEquals(0, records.size());
    }

    @Test
    public void tailMap_notExisting_butInRange() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(4, 301, 400);

        // WHEN & THEN (in between, exclusive)
        Set<Map.Entry> records = map.tailMap(heapData(3), false);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (in between, inclusive)
        records = map.tailMap(heapData(3), true);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (before, exclusive)
        records = map.tailMap(ess.toData(0, HEAP), false);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (before, inclusive)
        records = map.tailMap(ess.toData(0, HEAP), true);
        assertSegmentContent(records, 101, 400);
    }

    @Test
    public void exceptMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN
        Set<Map.Entry> records = map.exceptMap(heapData(1));
        assertSegmentContent(records, 201, 400);

        records = map.exceptMap(heapData(3));
        assertSegmentContent(records, 101, 300);

        records = map.exceptMap(heapData(4));
        assertSegmentContent(records, 101, 400);

        records = map.exceptMap(null);
        assertSegmentContent(records, 101, 400);

        records = map.exceptMap(new HeapData());
        assertSegmentContent(records, 101, 400);
    }

    private void assertSegmentContent(Set<Map.Entry> records, int expectedFromKey, int expectedToKey) {
        Set<Integer> foundKeys = new HashSet<Integer>();
        for (Map.Entry entry : records) {
            Integer key = ess.toObject(entry.getKey());
            String value = ess.toObject(entry.getValue());
            foundKeys.add(key);
            assertEquals("value:" + key, value);
        }

        assertEquals(expectedToKey - expectedFromKey + 1, records.size());
        assertEquals(expectedToKey - expectedFromKey + 1, foundKeys.size());
        for (int i = expectedFromKey; i <= expectedToKey; i++) {
            foundKeys.remove(i);
        }
        assertEquals(0, foundKeys.size());
    }

    private void putToSegment(Comparable segmentKey, int minValue, int maxValue) {
        Data segmentKeyData = ess.toData(segmentKey);
        int left = minValue;
        int right = maxValue;
        while (left < right) {
            map.put(segmentKeyData, nativeData(left), nativeData("value:" + left));
            map.put(segmentKeyData, nativeData(right), nativeData("value:" + right));
            left++;
            right--;
        }
    }

    private NativeMemoryData nativeData(Object object) {
        return ess.toData(object, NATIVE);
    }

    private HeapData heapData(Object object) {
        return ess.toData(object, HEAP);
    }

}
