package com.hazelcast.query.impl;

import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.elastic.map.BinaryElasticHashMap.loadFromOffHeapHeader;
import static com.hazelcast.nio.serialization.DataType.HEAP;

/**
 * Expects the entry.value to be in the NativeMemoryData format
 * Never disposes any NativeMemoryData passed to it.
 *
 * @param <T> type of the QueryableEntry entry passed to the map
 */
class HDIndexNestedHashMap<T extends QueryableEntry> {

    private static final long NULL_ADDRESS = 0L;

    private final BinaryElasticHashMap<NativeMemoryData> records;

    private final EnterpriseSerializationService ess;
    private final MemoryAllocator malloc;
    private final MapEntryFactory<T> mapEntryFactory;

    HDIndexNestedHashMap(EnterpriseSerializationService ess, MemoryAllocator malloc, MapEntryFactory<T> mapEntryFactory) {
        this.records = new BinaryElasticHashMap<NativeMemoryData>(ess, new NativeMemoryDataAccessor(ess), malloc);
        this.ess = ess;
        this.malloc = malloc;
        this.mapEntryFactory = mapEntryFactory != null ? mapEntryFactory : new DefaultMapEntryFactory<T>();
    }

    HDIndexNestedHashMap(EnterpriseSerializationService ess, MemoryAllocator malloc) {
        this(ess, malloc, null);
    }

    public NativeMemoryData put(Comparable segment, NativeMemoryData keyData, NativeMemoryData valueData) {
        Data segmentData = ess.toData(segment, HEAP);

        checkNotNullOrEmpty(segmentData, "segment can't be null or empty");
        checkNotNullOrEmpty(keyData, "key can't be null or empty");

        NativeMemoryData mapHeader = records.get(segmentData);

        BinaryElasticHashMap<NativeMemoryData> map;
        if (isNullOrEmptyData(mapHeader)) {
            map = new BinaryElasticHashMap<NativeMemoryData>(ess, new NativeMemoryDataAccessor(ess), malloc);
            mapHeader = map.storeHeaderOffHeap(malloc, NULL_ADDRESS);
            NativeMemoryData oldValue = records.put(segmentData, mapHeader);
            if (oldValue != null) {
                throw new ConcurrentModificationException();
            }
        } else {
            map = loadFromOffHeapHeader(ess, malloc, mapHeader.address());
        }

        if (valueData == null) {
            valueData = new NativeMemoryData();
        }
        NativeMemoryData oldValueData = map.put(keyData, valueData);
        map.storeHeaderOffHeap(malloc, mapHeader.address());

        return oldValueData;
    }

    /**
     * @return on-heap representation of the keys
     */
    public Set<Comparable> keySet() {
        Set<Comparable> result = new HashSet<Comparable>();
        for (Data data : records.keySet()) {
            Comparable value = ess.toObject(data, malloc);
            result.add(value);
        }
        return result;
    }

    /**
     * @param segment
     * @return on-heap representation of the entries in the segment
     */
    @SuppressWarnings("unchecked")
    public Set<T> get(Comparable segment) {
        Data segmentData = ess.toData(segment, HEAP);
        checkNotNullOrEmpty(segmentData, "segment can't be null or empty");

        NativeMemoryData mapHeader = records.get(segmentData);
        if (isNullOrEmptyData(mapHeader)) {
            return Collections.EMPTY_SET;
        }

        Set<T> result = new HashSet<T>();
        BinaryElasticHashMap<NativeMemoryData> map = loadFromOffHeapHeader(ess, malloc, mapHeader.address());
        for (Map.Entry<Data, NativeMemoryData> entry : map.entrySet()) {
            result.add(mapEntryFactory.create(
                    toHeapData((NativeMemoryData) entry.getKey()),
                    toHeapData(entry.getValue())));
        }
        return result;
    }

    public NativeMemoryData remove(Comparable segment, NativeMemoryData key) {
        Data segmentData = ess.toData(segment, HEAP);

        checkNotNullOrEmpty(segmentData, "segment can't be null or empty");
        checkNotNullOrEmpty(key, "key can't be null or empty");

        NativeMemoryData mapHeader = records.get(segmentData);
        if (isNullOrEmptyData(mapHeader)) {
            return null;
        }
        BinaryElasticHashMap<NativeMemoryData> map = loadFromOffHeapHeader(ess, malloc, mapHeader.address());
        // we are not disposing value - governed by the user of the map
        NativeMemoryData value = map.remove(key);
        if (map.isEmpty()) {
            map.dispose();
            NativeMemoryData mapHeaderOnRemove = records.remove(segmentData);
            ess.disposeData(mapHeaderOnRemove);
        } else {
            map.storeHeaderOffHeap(malloc, mapHeader.address());
        }
        return value;
    }

    public void clear() {
        for (NativeMemoryData mapHeader : records.values()) {
            if (isNullOrEmptyData(mapHeader)) {
                continue;
            }

            BinaryElasticHashMap map = loadFromOffHeapHeader(ess, malloc, mapHeader.address());
            map.clear();
            map.dispose();
        }

        records.clear();
    }

    public void dispose() {
        records.dispose();
    }

    public long size() {
        long size = 0;
        Iterator<NativeMemoryData> iterator = records.valueIter();
        while (iterator.hasNext()) {
            BinaryElasticHashMap map = loadFromOffHeapHeader(ess, malloc, iterator.next().address());
            size += map.size();
        }
        return size;
    }

    private boolean isNullOrEmptyData(Data data) {
        return data == null || data.totalSize() == 0;
    }

    private void checkNotNullOrEmpty(Data data, String message) {
        if (isNullOrEmptyData(data)) {
            throw new IllegalArgumentException(message);
        }
    }

    private Data toHeapData(NativeMemoryData nativeMemoryData) {
        if (isNullOrEmptyData(nativeMemoryData)) {
            return null;
        }
        return ess.toData(nativeMemoryData, HEAP);
    }

    private static class DefaultMapEntryFactory<T extends Map.Entry> implements MapEntryFactory<T> {
        @Override
        @SuppressWarnings("unchecked")
        public T create(Data key, Data value) {
            return (T) new AbstractMap.SimpleEntry(key, value);
        }
    }

}
