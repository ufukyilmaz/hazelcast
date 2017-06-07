package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.BinaryElasticNestedTreeMap;
import com.hazelcast.elastic.tree.ComparableComparator;
import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.Set;

import static com.hazelcast.nio.serialization.DataType.HEAP;

/**
 * Expects the entry.value to be in the NativeMemoryData format
 * Never disposes any NativeMemoryData passed to it.
 *
 * @param <T>
 */
class HDIndexNestedTreeMap<T extends QueryableEntry> {

    private final BinaryElasticNestedTreeMap<T> recordMap;
    private final EnterpriseSerializationService ess;

    HDIndexNestedTreeMap(EnterpriseSerializationService ess, MemoryAllocator malloc, MapEntryFactory<T> entryFactory) {
        this.ess = ess;
        this.recordMap = new BinaryElasticNestedTreeMap<T>(ess, malloc,
                new ComparableComparator(ess), entryFactory);
    }

    public NativeMemoryData put(Comparable attribute, NativeMemoryData key, NativeMemoryData value) {
        Data attributeData = ess.toData(attribute, HEAP);
        return recordMap.put(attributeData, key, value);
    }

    public NativeMemoryData remove(Comparable attribute, NativeMemoryData key) {
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

    public void clear() {
        recordMap.clear();
    }

    public void dispose() {
        recordMap.dispose();
    }

}
