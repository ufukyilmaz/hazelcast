package com.hazelcast.query.impl;

import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Unordered index store for HD memory.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key & value to be NativeMemoryData
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - There is no read & write locking since it's accessed from a single partition-thread only
 */
class HDUnorderedIndexStore extends BaseIndexStore {

    private final EnterpriseSerializationService ess;
    private final HDIndexHashMap<QueryableEntry> recordsWithNullValue;
    private final HDIndexNestedHashMap<QueryableEntry> records;

    HDUnorderedIndexStore(EnterpriseSerializationService ess, MemoryAllocator malloc,
                          MapEntryFactory<QueryableEntry> entryFactory) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(IndexCopyBehavior.NEVER);
        assertRunningOnPartitionThread();
        this.ess = ess;

        this.recordsWithNullValue = new HDIndexHashMap<QueryableEntry>(ess, malloc, entryFactory);
        this.records = new HDIndexNestedHashMap<QueryableEntry>(ess, malloc, entryFactory);
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
        assertRunningOnPartitionThread();
        if (value == NULL) {
            return recordsWithNullValue.entrySet();
        } else {
            return records.get(value);
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Comparable value : values) {
            results.addAll(getRecords(value));
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        assertRunningOnPartitionThread();
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Data valueData : records.keySet()) {
            Comparable indexedValue = ess.toObject(valueData);
            boolean valid;
            int result = value.compareTo(indexedValue);
            switch (comparison) {
                case LESS:
                    valid = result > 0;
                    break;
                case LESS_OR_EQUAL:
                    valid = result >= 0;
                    break;
                case GREATER:
                    valid = result < 0;
                    break;
                case GREATER_OR_EQUAL:
                    valid = result <= 0;
                    break;
                case NOT_EQUAL:
                    valid = result != 0;
                    break;
                default:
                    throw new IllegalStateException("Unrecognized comparison: " + comparison);
            }
            if (valid) {
                results.addAll(records.get(indexedValue));
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        assertRunningOnPartitionThread();
        if (from.compareTo(to) == 0) {
            if (!fromInclusive || !toInclusive) {
                return Collections.emptySet();
            }
            return records.get(from);
        }

        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        int fromBound = fromInclusive ? 0 : +1;
        int toBound = toInclusive ? 0 : -1;
        for (Data valueData : records.keySet()) {
            Comparable value = ess.toObject(valueData);
            if (value.compareTo(from) >= fromBound && value.compareTo(to) <= toBound) {
                results.addAll(records.get(value));
            }
        }
        return results;
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
        assertRunningOnPartitionThread();
        records.dispose();
        recordsWithNullValue.dispose();
    }

}
