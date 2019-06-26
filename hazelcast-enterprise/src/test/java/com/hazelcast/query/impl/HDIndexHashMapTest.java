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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.hazelcast.nio.serialization.DataType.NATIVE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HDIndexHashMapTest {

    private HazelcastMemoryManager malloc;
    private HDIndexHashMap<QueryableEntry> map;
    private EnterpriseSerializationService ess;
    private MapEntryFactory<QueryableEntry> factory;

    @Before
    public void setUp() {
        this.malloc = new StandardMemoryManager(new MemorySize(200, MemoryUnit.MEGABYTES));
        this.ess = getSerializationService();
        this.factory = new CachedQueryEntryFactory(ess);
        this.map = new HDIndexHashMap<QueryableEntry>(null, ess, malloc, factory);
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
    public void put_remove_size() throws IOException {
        // GIVEN
        QueryableEntry entry = entry(100, 200);

        // WHEN & THEN
        map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
        assertEquals(1, map.size());

        map.remove(entry.getKeyData());
        assertEquals(0, map.size());

        // THEN freed
        dispose(entry);
        map.dispose();
        assertNativeMemoryUsage(0);
    }

    @Test
    public void put() throws IOException {
        // GIVEN
        QueryableEntry entry = entry(100, 200);

        // WHEN
        map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());

        // THEN
        Set<QueryableEntry> entries = map.entrySet();
        assertEquals(1, entries.size());
        QueryableEntry entryGot = entries.iterator().next();

        assertEquals(entry.getKey(), entryGot.getKey());
        assertEquals(entry.getValue(), entryGot.getValue());

        // THEN freed
        dispose(entry);
        map.dispose();
        assertNativeMemoryUsage(0);
    }

    @Test
    public void put_nullValue() throws IOException {
        // GIVEN
        QueryableEntry entry = entry(100, null);

        // WHEN
        map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());

        // THEN
        Set<QueryableEntry> entries = map.entrySet();
        assertEquals(1, entries.size());
        QueryableEntry entryGot = entries.iterator().next();

        assertEquals(entry.getKey(), entryGot.getKey());
        assertEquals(entry.getValue(), entryGot.getValue());

        // THEN freed
        dispose(entry);
        map.dispose();
        assertNativeMemoryUsage(0);
    }

    @Test
    public void putMultiple() throws IOException {
        // WHEN
        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 1000; i++) {
            keys.add(i);
            QueryableEntry entry = entry(i, "value:" + i);
            map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
        }

        // THEN
        Set<QueryableEntry> entries = map.entrySet();
        assertEquals(1000, entries.size());
        for (QueryableEntry entry : entries) {
            keys.remove(entry.getKey());
            assertEquals(entry.getValue(), "value:" + entry.getKey());
        }
        assertEquals(0, keys.size());

    }

    @Test
    public void clear() throws IOException {
        // GIVEN
        HashSet<QueryableEntry> entries = new LinkedHashSet<QueryableEntry>();
        for (int i = 0; i < 1000; i++) {
            QueryableEntry entry = entry(i, "value:" + i);
            entries.add(entry);
            map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
        }
        assertEquals(1000, map.size());

        // WHEN
        map.clear();
        for (QueryableEntry entry : entries) {
            dispose(entry);
        }

        // THEN freeed
        assertEquals(0, map.size());
        map.dispose();
        assertNativeMemoryUsage(0);
    }

    @Test
    public void clear_empty() throws IOException {
        assertEquals(0, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void putClear_dispose_allDeallocated() throws IOException {
        // GIVEN
        HashSet<QueryableEntry> entries = new LinkedHashSet<QueryableEntry>();
        for (int i = 0; i < 1000; i++) {
            QueryableEntry entry = entry(i, "value:" + i);
            entries.add(entry);
            map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
        }

        // WHEN
        map.clear();
        for (QueryableEntry entry : entries) {
            dispose(entry);
        }

        // THEN
        map.dispose();
        assertNativeMemoryUsage(0);
    }

    @Test
    public void putRemove_dispose_allDeallocated() throws IOException {
        // GIVEN
        HashSet<QueryableEntry> entries = new LinkedHashSet<QueryableEntry>();
        for (int i = 0; i < 1000; i++) {
            QueryableEntry entry = entry(i, "value:" + i);
            map.put((NativeMemoryData) entry.getKeyData(), (NativeMemoryData) entry.getValueData());
            entries.add(entry);
        }

        // WHEN
        for (QueryableEntry entry : entries) {
            map.remove(entry.getKeyData());
            dispose(entry);
        }

        // THEN
        map.dispose();
        assertNativeMemoryUsage(0);
    }

    public void dispose(QueryableEntry entry) {
        ess.disposeData(entry.getKeyData());
        ess.disposeData(entry.getValueData());
    }

    private QueryableEntry entry(Object key, Object value) {
        return new CachedQueryEntry(ess, ess.toData(key, NATIVE),
                ess.toData(value, NATIVE), Extractors.newBuilder(ess).build());
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
