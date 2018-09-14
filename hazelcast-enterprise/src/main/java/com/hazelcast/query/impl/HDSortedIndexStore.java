package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Sorted index store for HD memory.
 *
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key & value to be NativeMemoryData
 * - Whenever Data is passed to it (removeIndexInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
class HDSortedIndexStore extends BaseIndexStore {

    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedTreeMap<QueryableEntry> records;

    HDSortedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc, MapEntryFactory<QueryableEntry> entryFactory) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER);
        assertRunningOnPartitionThread();

        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, entryFactory);
        this.records = new HDIndexNestedTreeMap<QueryableEntry>(ess, malloc, entryFactory);
    }

    @Override
    Object newIndexInternal(Comparable newValue, QueryableEntry entry) {
        assertRunningOnPartitionThread();
        if (newValue instanceof IndexImpl.NullObject) {
            NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
            NativeMemoryData value = (NativeMemoryData) entry.getValueData();
            return recordsWithNullValue.put(key, value);
        } else {
            return mapAttributeToEntry(newValue, entry);
        }
    }

    private Object mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
        NativeMemoryData value = (NativeMemoryData) entry.getValueData();
        return records.put(attribute, key, value);
    }

    @Override
    Object removeIndexInternal(Comparable oldValue, Data recordKey) {
        assertRunningOnPartitionThread();
        if (oldValue instanceof IndexImpl.NullObject) {
            return recordsWithNullValue.remove(recordKey);
        } else {
            return removeMappingForAttribute(oldValue, recordKey);
        }
    }

    private Object removeMappingForAttribute(Comparable attribute, Data indexKey) {
        return records.remove(attribute, (NativeMemoryData) indexKey);
    }

    @Override
    public void clear() {
        assertRunningOnPartitionThread();
        recordsWithNullValue.clear();
        records.clear();
    }

    private void dispose() {
        recordsWithNullValue.dispose();
        records.dispose();
    }

    @Override
    public void destroy() {
        assertRunningOnPartitionThread();
        clear();
        dispose();
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        assertRunningOnPartitionThread();
        return records.subMap(from, true, to, true);
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results;
        switch (comparisonType) {
            case LESSER:
                results = records.headMap(searchedValue, false);
                break;
            case LESSER_EQUAL:
                results = records.headMap(searchedValue, true);
                break;
            case GREATER:
                results = records.tailMap(searchedValue, false);
                break;
            case GREATER_EQUAL:
                results = records.tailMap(searchedValue, true);
                break;
            case NOT_EQUAL:
                results = records.exceptMap(searchedValue);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparisonType: " + comparisonType);
        }
        return results;
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

    private Set<QueryableEntry> doGetRecords(Comparable value) {
        assertRunningOnPartitionThread();
        if (value instanceof IndexImpl.NullObject) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

    @Override
    public String toString() {
        return "HDSortedIndexStore{"
                + "recordMap=" + records.hashCode()
                + '}';
    }

}
