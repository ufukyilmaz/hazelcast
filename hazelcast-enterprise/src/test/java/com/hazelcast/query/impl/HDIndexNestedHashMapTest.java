package com.hazelcast.query.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.StandardMemoryManager;
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

import static com.hazelcast.nio.serialization.DataType.HEAP;
import static com.hazelcast.nio.serialization.DataType.NATIVE;
import static com.hazelcast.query.impl.BaseIndexStore.LOAD_FACTOR;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HDIndexNestedHashMapTest {

    private HazelcastMemoryManager malloc;
    private HDIndexNestedHashMap<QueryableEntry> map;
    private EnterpriseSerializationService ess;
    private MapEntryFactory factory;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(2, MemoryUnit.GIGABYTES));
        this.ess = getSerializationService();
        this.factory = new CachedQueryEntryFactory(ess);
        this.map = new HDIndexNestedHashMap<QueryableEntry>(null, ess, malloc, factory);
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

            NativeMemoryData keyNative = ess.toData(key, NATIVE);
            map.remove(segmentKey, keyNative);
            assertEquals(0, map.size());
            ess.disposeData(keyNative);
        }
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

        System.err.println("HashMap count = " + count);
    }

    @Ignore
    @Test
    public void perfTestGet_onHeap() {

        ConcurrentMap<Comparable, ConcurrentMap<Data, QueryableEntry>> recordMap
                = new ConcurrentHashMap<Comparable, ConcurrentMap<Data, QueryableEntry>>(1000);

        int count = 100000;
        long durationInMillis = 30 * 1000;
        System.err.println("Setup started");
        for (int j = 0; j < count; j++) {
            String k = 0 + "" + j;

            QueryableEntry entry = new CachedQueryEntry(ess, ess.toData(k, HEAP), ess.toData("v" + k, HEAP), newExtractors());

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

    protected Extractors newExtractors() {
        return Extractors.newBuilder(ess).build();
    }

    @Ignore
    @Test
    public void perfTestGet() {
        int count = 100000;
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

    @Test
    public void twoEntries_putRemove_dispose_allDeallocated() throws IOException {
        // GIVEN
        QueryableEntry e1 = entry(1, "value:" + 1);
        QueryableEntry e2 = entry(2, "value:" + 1);
        map.put(1, (NativeMemoryData) e1.getKeyData(), (NativeMemoryData) e1.getValueData());
        map.put(1, (NativeMemoryData) e2.getKeyData(), (NativeMemoryData) e2.getValueData());

        // WHEN
        map.remove(1, (NativeMemoryData) e1.getKeyData());
        map.remove(1, (NativeMemoryData) e2.getKeyData());

        // THEN
        dispose(e1);
        dispose(e2);
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

    private void dispose(QueryableEntry entry) {
        ess.disposeData(entry.getKeyData());
        ess.disposeData(entry.getValueData());
    }

    private void assertNativeMemoryUsage(int expected) {
        assertEquals(expected, malloc.getMemoryStats().getUsedNative());
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
