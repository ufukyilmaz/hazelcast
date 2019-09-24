package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.tree.ComparableComparator;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.Set;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * Wrapper around BinaryElasticNestedTreeMap for the usage in IndexStores
 * Does validation and necessary transformations.
 *
 * Contract:
 * - Expects the key to be in the NativeMemoryData,
 * - Expects the value to be in either NativeMemoryData or HDRecord,
 * - Returns NativeMemoryData,
 * - Never disposes any NativeMemoryData passed to it,
 * - Uses MapEntryFactory to create MapEntry instances in methods that return them.
 *
 * @param <T> type of the QueryableEntry entry passed to the map
 */
class HDIndexNestedTreeMap<T extends QueryableEntry> {

    private final HDIndexBinaryElasticNestedTreeMap<T> recordMap;
    private final EnterpriseSerializationService ess;

    HDIndexNestedTreeMap(HDExpirableIndexStore indexStore, EnterpriseSerializationService ess, MemoryAllocator malloc,
                         MapEntryFactory<T> entryFactory) {
        this.ess = ess;
        this.recordMap = new HDIndexBinaryElasticNestedTreeMap<T>(indexStore, ess, malloc,
                new ComparableComparator(ess), entryFactory
        );
    }

    public MemoryBlock put(Comparable attribute, NativeMemoryData key, MemoryBlock value) {
        Data attributeData = ess.toData(attribute, HEAP);
        return recordMap.put(attributeData, key, value);
    }

    public MemoryBlock remove(Comparable attribute, NativeMemoryData key) {
        return recordMap.remove(ess.toData(attribute, HEAP), key);
    }

    public Set<T> subMap(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        Data fromData = ess.toData(from, HEAP);
        Data toData = ess.toData(to, HEAP);
        return recordMap.subMap(fromData, fromInclusive, toData, toInclusive);
    }

    public Set<T> headMap(Comparable toValue, boolean inclusive) {
        Data toValueData = ess.toData(toValue, HEAP);
        return recordMap.headMap(toValueData, inclusive);
    }

    public Set<T> tailMap(Comparable fromValue, boolean inclusive) {
        Data fromValueData = ess.toData(fromValue, HEAP);
        return recordMap.tailMap(fromValueData, inclusive);
    }

    public Set<T> exceptMap(Comparable exceptValue) {
        Data exceptValueData = ess.toData(exceptValue, HEAP);
        return recordMap.exceptMap(exceptValueData);
    }

    public Set<T> get(Comparable searchedValue) {
        Data searchedValueData = ess.toData(searchedValue, HEAP);
        return recordMap.get(searchedValueData);
    }

    public long size() {
        return recordMap.size();
    }

    /**
     * Clears the map by removing and disposing all segments including key/value pairs stored.
     */
    public void clear() {
        recordMap.clear();
    }

    /**
     * Disposes internal backing BinaryElasticNestedTreeMap. Does not dispose segments nor key/value pairs inside.
     * To dispose key/value pairs, {@link #clear()} must be called explicitly.
     *
     * @see #clear()
     */
    public void dispose() {
        recordMap.dispose();
    }

}
