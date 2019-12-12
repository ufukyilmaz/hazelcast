package com.hazelcast.query.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.hazelcast.nio.serialization.DataType.HEAP;
import static com.hazelcast.nio.serialization.DataType.NATIVE;
import static com.hazelcast.query.impl.BaseSingleValueIndexStore.LOAD_FACTOR;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HDIndexNestedTreeMapTest {

    private HazelcastMemoryManager malloc;
    private HDIndexNestedTreeMap<QueryableEntry> map;
    private EnterpriseSerializationService ess;
    private MapEntryFactory factory;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(2, MemoryUnit.GIGABYTES));
        this.ess = getSerializationService();
        this.factory = new CachedQueryEntryFactory(ess);
        this.map = new HDIndexNestedTreeMap<QueryableEntry>(ess, malloc, factory);
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
    public void get_segment() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN
        Set<QueryableEntry> records = map.get(1);

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
        Set<QueryableEntry> records = map.get(4);

        // THEN
        assertEquals(0, records.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void get_segment_nullSegmentKey() throws IOException {
        // GIVEN
        putToSegment(1, 101, 200);

        // WHEN
        Set<QueryableEntry> records = map.get(null);
    }

    @Test
    public void getSet() throws IOException {
        Comparable segmentKey = 1;
        Object key = 100;
        Object value = 200;

        QueryableEntry entry = entry(key, value);
        map.put(segmentKey, (NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());

        Set<QueryableEntry> entries = map.get(segmentKey);
        assertEquals(1, entries.size());
        assertEquals(value, entries.iterator().next().getValue());
    }

    @Test
    public void getSet_nullValue() throws IOException {
        Comparable segmentKey = 1;
        Object key = 100;
        Object value = null;

        QueryableEntry entry = entry(key, value);
        map.put(segmentKey, (NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());

        Set<QueryableEntry> entries = map.get(segmentKey);
        assertEquals(1, entries.size());
        assertEquals(value, entries.iterator().next().getValue());
    }

    @Test
    public void sizeAndClearOnEmpty() throws IOException {
        assertEquals(0, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void put_get_remove_multipleTimes() throws IOException {
        Comparable segmentKey = 1;
        Object key = 100;
        Object value = 200;

        for (int i = 1; i <= 10000; i++) {
            QueryableEntry entry = entry(key, value);
            map.put(segmentKey, (NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
            assertEquals(1, map.size());

            Set<QueryableEntry> entries = map.get(segmentKey);
            assertEquals(1, entries.size());
            assertEquals(value, entries.iterator().next().getValue());

            map.remove(segmentKey, (NativeMemoryData) ess.toData(key, NATIVE));
            assertEquals(0, map.size());
        }
    }

    @Test
    public void put_get_batch_multipleTimes() throws IOException {
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                String k = i + "" + j;
                QueryableEntry entry = entry(k, "v" + k);
                map.put(i, (NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
            }
        }

        for (int i = 0; i < 10; i++) {
            Set<QueryableEntry> entries = map.get(i);
            assertEquals(10, entries.size());
            for (int j = 9; j >= 0; j--) {
                String k = i + "" + j;
                entries.remove(entry(k, "v" + k));
            }
            assertEquals(0, entries.size());
        }

        for (int i = 9; i >= 0; i--) {
            Set<QueryableEntry> entries = map.get(i);
            assertEquals(10, entries.size());
            for (int j = 9; j >= 0; j--) {
                String k = i + "" + j;
                entries.remove(entry(k, "v" + k));
            }
            assertEquals(0, entries.size());
        }
    }

    @Ignore
    @Test
    public void perfTestPut() {
        long start = System.currentTimeMillis();
        long count = 0L;
        int i = 0;
        while (System.currentTimeMillis() - start < 10 * 1000) {
            for (int j = 0; j < 1 && (System.currentTimeMillis() - start < 10 * 1000); j++) {
                String k = i + "" + j;
                QueryableEntry entry = entry(k, "v" + k);
                map.put(i++, (NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
                count++;
            }
        }

        System.err.println("TreeMap count = " + count);
    }

    @Ignore
    @Test
    public void perfTestGet_onHeap() {

        ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>> recordMap
                = new ConcurrentSkipListMap<Comparable, ConcurrentMap<Data, QueryableEntry>>();

        int count = 100000;
        long durationInMillis = 30 * 1000;
        System.err.println("Setup started");
        for (int j = 0; j < count; j++) {
            String k = 0 + "" + j;

            QueryableEntry entry = new CachedQueryEntry(ess, ess.toData(k, HEAP), ess.toData("v" + k, HEAP),
                    Extractors.newBuilder(ess).build());

            ConcurrentHashMap m = new ConcurrentHashMap<Data, QueryableEntry>(1, LOAD_FACTOR, 1);
            m.put(entry.getKeyData(), entry);
            recordMap.put(j, m);
        }
        System.err.println("Setup finished");

        System.err.println("Test started");
        long start = System.currentTimeMillis();
        int j = 0;
        int resultCount = 0;
        while (System.currentTimeMillis() - start < durationInMillis) {
            String k = 0 + "" + j;
            recordMap.get(j).get(ess.toData(k, HEAP));
            j = (j + 1) % count;
            resultCount++;
        }
        System.err.println("Test finished");
        System.err.println("Count for " + count + " elements = " + resultCount);
    }

    @Ignore
    @Test
    public void perfTestGet() {
        int count = 1000000;
        long durationInMillis = 30 * 1000;
        System.err.println("Setup started");
        for (int j = 0; j < count; j++) {
            String k = 0 + "" + j;
            QueryableEntry entry = entry(k, "v" + k);
            map.put(j, (NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
        }
        System.err.println("Setup finished");

        System.err.println("Test started");
        long start = System.currentTimeMillis();
        int j = 0;
        int resultCount = 0;
        while (System.currentTimeMillis() - start < durationInMillis) {
            map.get(j);
            j = (j + 1) % count;
            resultCount++;
        }
        System.err.println("Test finished");
        System.err.println("Count for " + count + " elements = " + resultCount);
    }


    @Test
    public void subMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN (exclusive, inclusive)
        Set<QueryableEntry> records = map.subMap(2, false, 3, true);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (inclusive, exclusive)
        records = map.subMap(2, true, 3, false);
        assertSegmentContent(records, 201, 300);

        // WHEN & THEN (inclusive, inclusive)
        records = map.subMap(1, true, 3, true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (exclusive, exclusive)
        records = map.subMap(1, false, 3, false);
        assertSegmentContent(records, 201, 300);

        // WHEN & THEN (exclusive, exclusive) -> empty
        records = map.subMap(1, false, 1, false);
        assertEquals(0, records.size());

        // WHEN & THEN (inclusive, not found)
        records = map.subMap(2, true, 5, false);
        assertSegmentContent(records, 201, 400);

        // WHEN & THEN (exclusive last, not found)
        records = map.subMap(3, false, 5, false);
        assertEquals(0, records.size());

        // WHEN & THEN (not found, ...)
        records = map.subMap(140, true, 5, false);
        assertEquals(0, records.size());

        // WHEN & THEN (both null & inclusive)
        records = map.subMap(null, true, null, true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (both null & exclusive)
        records = map.subMap(null, false, null, false);
        assertSegmentContent(records, 101, 400);
    }

    @Test
    public void headMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN (exclusive)
        Set<QueryableEntry> records = map.headMap(2, false);
        assertSegmentContent(records, 101, 200);

        // WHEN & THEN (inclusive)
        records = map.headMap(2, true);
        assertSegmentContent(records, 101, 300);

        // WHEN & THEN (inclusive, edge)
        records = map.headMap(1, true);
        assertSegmentContent(records, 101, 200);

        // WHEN & THEN (exclusive, edge)
        records = map.headMap(1, false);
        assertEquals(0, records.size());

        // WHEN & THEN (exclusive, non-existing)
        records = map.headMap(123, false);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (inclusive, non-existing)
        records = map.headMap(123, true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (inclusive, null)
        records = map.headMap(null, true);
        assertSegmentContent(records, 101, 400);

        // WHEN & THEN (exclusive, null)
        records = map.headMap(null, false);
        assertSegmentContent(records, 101, 400);
    }

    @Test
    public void tailMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN (exclusive)
        Set<QueryableEntry> records = map.tailMap(2, false);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (inclusive)
        records = map.tailMap(2, true);
        assertSegmentContent(records, 201, 400);

        // WHEN & THEN (inclusive, edge)
        records = map.tailMap(3, true);
        assertSegmentContent(records, 301, 400);

        // WHEN & THEN (exclusive, edge)
        records = map.tailMap(3, false);
        assertEquals(0, records.size());

        // WHEN & THEN (exclusive, non-existing)
        records = map.tailMap(123, false);
        assertEquals(0, records.size());

        // WHEN & THEN (inclusive, non-existing)
        records = map.tailMap(123, true);
        assertEquals(0, records.size());

        // WHEN & THEN (exclusive, null)
        records = map.tailMap(null, false);
        assertEquals(0, records.size());

        // WHEN & THEN (inclusive, null)
        records = map.tailMap(null, true);
        assertEquals(0, records.size());
    }

    @Test
    public void exceptMap() throws IOException {
        // GIVEN
        putToSegment(2, 201, 300);
        putToSegment(1, 101, 200);
        putToSegment(3, 301, 400);

        // WHEN & THEN
        Set<QueryableEntry> records = map.exceptMap(1);
        assertSegmentContent(records, 201, 400);

        records = map.exceptMap(3);
        assertSegmentContent(records, 101, 300);

        records = map.exceptMap(4);
        assertSegmentContent(records, 101, 400);

        records = map.exceptMap(null);
        assertSegmentContent(records, 101, 400);
    }


    @Test
    public void putClear_dispose_allDeallocated() throws IOException {
        List<QueryableEntry> localAllocations = new ArrayList<QueryableEntry>();

        // GIVEN
        for (int i = 0; i < 1000; i++) {
            QueryableEntry e = entry(i, "value:" + i);
            localAllocations.add(e);
            map.put(i, (NativeMemoryData) e.getKeyData(), (NativeMemoryData) e.getValueData());
        }

        // WHEN
        map.clear();
        map.dispose();
        for (QueryableEntry entry : localAllocations) {
            dispose(entry);
        }

        // THEN
        assertNativeMemoryUsage(0);
    }

    @Test
    public void putRemove_dispose_allDeallocated() throws IOException {
        // GIVEN
        HashSet<QueryableEntry> entries = new LinkedHashSet<QueryableEntry>();
        for (int i = 0; i < 1000; i++) {
            QueryableEntry e = entry(i, "value:" + i);
            map.put(i, (NativeMemoryData) e.getKeyData(), (NativeMemoryData) e.getValueData());
            entries.add(e);
        }

        // WHEN
        int i = 0;
        for (QueryableEntry entry : entries) {
            map.remove(i++, (NativeMemoryData) entry.getKeyData());
            dispose(entry);
        }

        // THEN
        map.dispose();
        assertNativeMemoryUsage(0);
    }


    private void assertSegmentContent(Set<QueryableEntry> records, int expectedFromKey, int expectedToKey) {
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
        int left = minValue;
        int right = maxValue;
        while (left < right) {
            QueryableEntry leftEntry = entry(left, "value:" + left);
            QueryableEntry rightEntry = entry(right, "value:" + right);

            map.put(segmentKey, (NativeMemoryData) leftEntry.getKeyData(), (NativeMemoryData) leftEntry.getValueData());
            map.put(segmentKey, (NativeMemoryData) rightEntry.getKeyData(), (NativeMemoryData) rightEntry.getValueData());
            left++;
            right--;
        }
    }

    private QueryableEntry entry(Object key, Object value) {
        return new CachedQueryEntry(ess, ess.toData(key, NATIVE),
                ess.toData(value, NATIVE), Extractors.newBuilder(ess).build());
    }

    private void assertNativeMemoryUsage(int expected) {
        assertEquals(expected, malloc.getMemoryStats().getUsedNative());
    }

    private void dispose(QueryableEntry entry) {
        ess.disposeData(entry.getKeyData());
        ess.disposeData(entry.getValueData());
    }

    private static class CachedQueryEntryFactory implements MapEntryFactory<QueryableEntry> {
        private final EnterpriseSerializationService ess;

        CachedQueryEntryFactory(EnterpriseSerializationService ess) {
            this.ess = ess;
        }

        @Override
        public CachedQueryEntry create(Data key, Data value) {
            return new CachedQueryEntry(ess, key, value, Extractors.newBuilder(ess).build());
        }
    }
}
