package com.hazelcast.query.impl;

import com.hazelcast.internal.bplustree.BPlusTreeKeyAccessor;
import com.hazelcast.internal.bplustree.BPlusTreeKeyComparator;
import com.hazelcast.internal.bplustree.HDBPlusTree;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import java.util.Iterator;

/**
 * Wrapper around HDBPlusTree for the usage in IndexStores
 *
 * Contract:
 * - Expects the entry key to be in the NativeMemoryData,
 * - Expects the value to be in either NativeMemoryData or HDRecord,
 * - Returns NativeMemoryData,
 * - Never disposes any NativeMemoryData passed to it,
 * - Uses entryFactory to create on-heap QueryableEntry instances in methods that return them.
 *
 * @param <T> the type of the QueryableEntry entries
 */
public final class HDBPlusTreeIndex<T extends QueryableEntry> {

    private final EnterpriseSerializationService ess;
    private final HDBPlusTree recordMap;

    HDBPlusTreeIndex(EnterpriseSerializationService ess, MemoryAllocator keyAllocator, MemoryAllocator indexAllocator,
                     MapEntryFactory<T> entryFactory, BPlusTreeKeyComparator keyComparator,
                     BPlusTreeKeyAccessor keyAccessor, int nodeSize) {
        this.ess = ess;
        this.recordMap = HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator,
                keyComparator, keyAccessor, entryFactory, nodeSize);
    }

    public MemoryBlock put(Comparable attribute, NativeMemoryData key, MemoryBlock value) {
        return recordMap.insert(attribute, key, value);
    }

    public MemoryBlock remove(Comparable attribute, NativeMemoryData key) {
        return recordMap.remove(attribute, key);
    }

    public Iterator<QueryableEntry> getKeysInRange(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        return recordMap.lookup(from, fromInclusive, to, toInclusive);
    }

    public Iterator<QueryableEntry> lookup(Comparable value) {
        return recordMap.lookup(value, true, value, true);
    }

    public void clear() {
        recordMap.clear();
    }

    public void dispose() {
        recordMap.dispose();
    }
}
