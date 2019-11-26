package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.map.BehmSlotAccessorFactory;
import com.hazelcast.internal.elastic.map.BinaryElasticHashMap;
import com.hazelcast.internal.elastic.map.NativeBehmSlotAccessorFactory;
import com.hazelcast.internal.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.nio.serialization.Data;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.elastic.map.BinaryElasticHashMap.loadFromOffHeapHeader;
import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * Nested map, so a map of maps, with two-tiers of keys.
 * First tier maps keys to segments. Each segment maps keys to values.
 * There is not sorting of keys in none of the tiers.
 *
 * Uses BinaryElasticHashMap as the underlying store for the segments.
 * Each segment is stored under a segmentKey passed as Data (may be on-heap of off-heap Data).
 * There is one segment per segmentKey.
 *
 * Each segment uses BinaryElasticHashMap as the underlying segment storage.
 *
 * Contract of each segment:
 * - Expects the key & value to be in the NativeMemoryData,
 * - Returns NativeMemoryData,
 * - Never disposes any NativeMemoryData passed to it,
 *
 * Each method that returns MapEntry instances uses MapEntryFactory to create them.
 *
 * @param <T> type of the Map.Entry returned by the tree.
 */
class HDIndexNestedHashMap<T extends QueryableEntry> {

    private static final long NULL_ADDRESS = 0L;

    private final BinaryElasticHashMap<NativeMemoryData> records;

    private final EnterpriseSerializationService ess;
    private final MemoryAllocator malloc;
    private final MapEntryFactory<T> mapEntryFactory;
    private final HDExpirableIndexStore indexStore;
    private final BehmSlotAccessorFactory behmSlotAccessorFactory;
    private final MemoryBlockAccessor behmMemoryBlockAccessor;

    HDIndexNestedHashMap(HDExpirableIndexStore indexStore, EnterpriseSerializationService ess, MemoryAllocator malloc,
                         MapEntryFactory<T> mapEntryFactory) {
        this.indexStore = indexStore;
        this.records = new BinaryElasticHashMap<NativeMemoryData>(ess, new NativeBehmSlotAccessorFactory(),
                new NativeMemoryDataAccessor(ess), malloc);
        this.ess = ess;
        this.malloc = malloc;
        this.mapEntryFactory = mapEntryFactory;
        this.behmSlotAccessorFactory = new HDIndexBehmSlotAccessorFactory();
        MemoryBlockAccessor valueAccessor = new NativeMemoryDataAccessor(ess);
        MemoryBlockAccessor recordAccessor = new HDRecordAccessor(ess);
        this.behmMemoryBlockAccessor = new HDIndexBehmMemoryBlockAccessor(valueAccessor, recordAccessor);
    }

    public MemoryBlock put(Comparable segment, NativeMemoryData keyData, MemoryBlock value) {
        Data segmentData = ess.toData(segment, HEAP);

        checkNotNullOrEmpty(segmentData, "segment can't be null or empty");
        checkNotNullOrEmpty(keyData, "key can't be null or empty");

        NativeMemoryData mapHeader = records.get(segmentData);

        BinaryElasticHashMap<MemoryBlock> map;
        if (isNullOrEmptyData(mapHeader)) {
            map = new BinaryElasticHashMap<MemoryBlock>(ess, behmSlotAccessorFactory, behmMemoryBlockAccessor, malloc);
            mapHeader = map.storeHeaderOffHeap(malloc, NULL_ADDRESS);
            NativeMemoryData oldValue = records.put(segmentData, mapHeader);
            if (oldValue != null) {
                throw new ConcurrentModificationException();
            }
        } else {
            map = loadFromOffHeapHeader(ess, malloc, mapHeader.address(), behmSlotAccessorFactory, behmMemoryBlockAccessor);
        }

        if (value == null) {
            value = new NativeMemoryData();
        }
        MemoryBlock oldValueData = map.put(keyData, value);
        map.storeHeaderOffHeap(malloc, mapHeader.address());

        return oldValueData;
    }

    /**
     * @return off-heap set of keys
     */
    @SuppressWarnings("unchecked")
    public Set<NativeMemoryData> keySet() {
        Set keySet = records.keySet();
        return ((Set<NativeMemoryData>) keySet);
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
            return Collections.emptySet();
        }

        Set<T> result = new HashSet<T>();
        BinaryElasticHashMap<MemoryBlock> map = loadFromOffHeapHeader(ess, malloc, mapHeader.address(), behmSlotAccessorFactory,
                behmMemoryBlockAccessor);
        for (Map.Entry<Data, MemoryBlock> entry : map.entrySet()) {
            Data key = entry.getKey();
            MemoryBlock memoryBlock = entry.getValue();
            NativeMemoryData valueData;
            if (memoryBlock instanceof NativeMemoryData || memoryBlock == null) {
                valueData = (NativeMemoryData) memoryBlock;
            } else {
                valueData = indexStore.getValueOrNullIfExpired(key, memoryBlock);
            }

            if (memoryBlock == null || valueData != null) {
                result.add(mapEntryFactory.create(entry.getKey(), valueData));
            }
        }
        return result;
    }

    public MemoryBlock remove(Comparable segment, NativeMemoryData key) {
        Data segmentData = ess.toData(segment, HEAP);

        checkNotNullOrEmpty(segmentData, "segment can't be null or empty");
        checkNotNullOrEmpty(key, "key can't be null or empty");

        NativeMemoryData mapHeader = records.get(segmentData);
        if (isNullOrEmptyData(mapHeader)) {
            return null;
        }
        BinaryElasticHashMap<MemoryBlock> map = loadFromOffHeapHeader(ess, malloc, mapHeader.address(), behmSlotAccessorFactory,
                behmMemoryBlockAccessor);
        // we are not disposing value - governed by the user of the map
        MemoryBlock value = map.remove(key);
        if (map.isEmpty()) {
            map.dispose();
            NativeMemoryData mapHeaderOnRemove = records.remove(segmentData);
            ess.disposeData(mapHeaderOnRemove);
        } else {
            map.storeHeaderOffHeap(malloc, mapHeader.address());
        }
        return value;
    }

    /**
     * Clears the map by removing and disposing all segments including key/value pairs stored.
     */
    public void clear() {
        for (NativeMemoryData mapHeader : records.values()) {
            if (isNullOrEmptyData(mapHeader)) {
                continue;
            }

            BinaryElasticHashMap map = loadFromOffHeapHeader(ess, malloc, mapHeader.address(), behmSlotAccessorFactory,
                    behmMemoryBlockAccessor);
            map.dispose();
        }

        records.clear();
    }

    /**
     * Disposes internal backing top-level BEHM. Does not dispose segments nor key/value pairs inside.
     * To dispose key/value pairs, {@link #clear()} must be called explicitly.
     *
     * @see #clear()
     */
    public void dispose() {
        records.dispose();
    }

    public long size() {
        long size = 0;
        Iterator<NativeMemoryData> iterator = records.valueIter();
        while (iterator.hasNext()) {
            BinaryElasticHashMap map = loadFromOffHeapHeader(ess, malloc, iterator.next().address(), behmSlotAccessorFactory,
                    behmMemoryBlockAccessor);
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
}
