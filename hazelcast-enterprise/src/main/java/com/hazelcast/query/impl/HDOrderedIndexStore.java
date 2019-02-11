package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Ordered index store for HD memory.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key & value to be NativeMemoryData
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
class HDOrderedIndexStore extends BaseIndexStore {

    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedTreeMap<QueryableEntry> records;

    HDOrderedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc,
                        MapEntryFactory<QueryableEntry> entryFactory) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER);
        assertRunningOnPartitionThread();

        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, entryFactory);
        this.records = new HDIndexNestedTreeMap<QueryableEntry>(ess, malloc, entryFactory);
    }

    @Override
    Object insertInternal(Comparable newValue, QueryableEntry entry) {
        assertRunningOnPartitionThread();
        if (newValue == NULL) {
            NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
            NativeMemoryData value = (NativeMemoryData) entry.getValueData();
            return recordsWithNullValue.put(key, value);
        } else {
            return mapAttributeToEntry(newValue, entry);
        }
    }

    @Override
    Object removeInternal(Comparable value, Data recordKey) {
        assertRunningOnPartitionThread();
        if (value == NULL) {
            return recordsWithNullValue.remove(recordKey);
        } else {
            return removeMappingForAttribute(value, recordKey);
        }
    }

    @Override
    public void destroy() {
        assertRunningOnPartitionThread();
        clear();
        dispose();
    }

    @Override
    public void clear() {
        assertRunningOnPartitionThread();
        recordsWithNullValue.clear();
        records.clear();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return doGetRecords(value);
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Comparable value : values) {
            results.addAll(doGetRecords(value));
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results;
        switch (comparison) {
            case LESS:
                results = records.headMap(value, false);
                break;
            case LESS_OR_EQUAL:
                results = records.headMap(value, true);
                break;
            case GREATER:
                results = records.tailMap(value, false);
                break;
            case GREATER_OR_EQUAL:
                results = records.tailMap(value, true);
                break;
            case NOT_EQUAL:
                results = records.exceptMap(value);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        assertRunningOnPartitionThread();
        return records.subMap(from, fromInclusive, to, toInclusive);
    }

    private Object mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
        NativeMemoryData value = (NativeMemoryData) entry.getValueData();
        return records.put(attribute, key, value);
    }

    private Object removeMappingForAttribute(Comparable attribute, Data indexKey) {
        return records.remove(attribute, (NativeMemoryData) indexKey);
    }

    private void dispose() {
        recordsWithNullValue.dispose();
        records.dispose();
    }

    private Set<QueryableEntry> doGetRecords(Comparable value) {
        assertRunningOnPartitionThread();
        if (value == NULL) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

}
