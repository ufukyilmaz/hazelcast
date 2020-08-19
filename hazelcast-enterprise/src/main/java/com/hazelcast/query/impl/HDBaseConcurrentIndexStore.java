package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.bplustree.BPlusTreeInlinedLongAccessor;
import com.hazelcast.internal.bplustree.BPlusTreeInlinedLongComparator;
import com.hazelcast.internal.bplustree.BPlusTreeKeyComparator;
import com.hazelcast.internal.bplustree.DefaultBPlusTreeKeyAccessor;
import com.hazelcast.internal.bplustree.EntrySlotNoPayload;
import com.hazelcast.internal.bplustree.EntrySlotPayload;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.query.Predicate;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

abstract class HDBaseConcurrentIndexStore extends BaseSingleValueIndexStore {

    protected final HDBPlusTreeIndex<QueryableEntry> recordsWithNullValue;
    protected final HDBPlusTreeIndex<QueryableEntry> records;

    HDBaseConcurrentIndexStore(IndexCopyBehavior copyBehavior,
                               EnterpriseSerializationService ess,
                               MemoryAllocator keyAllocator,
                               MemoryAllocator indexAllocator,
                               BPlusTreeKeyComparator keyComparator,
                               MapEntryFactory<QueryableEntry> entryFactory,
                               int nodeSize,
                               EntrySlotPayload entrySlotPayload) {
        super(copyBehavior, false);

        this.recordsWithNullValue = new HDBPlusTreeIndex(ess, keyAllocator, indexAllocator, entryFactory,
                new BPlusTreeInlinedLongComparator(), new BPlusTreeInlinedLongAccessor(ess), nodeSize,
                new EntrySlotNoPayload());
        try {
            this.records = new HDBPlusTreeIndex(ess, keyAllocator, indexAllocator, entryFactory,
                    keyComparator, new DefaultBPlusTreeKeyAccessor(ess), nodeSize,
                    entrySlotPayload);
        } catch (NativeOutOfMemoryError e) {
            recordsWithNullValue.dispose();
            throw e;
        }
    }

    @Override
    Object insertInternal(Comparable newValue, QueryableEntry entry) {
        if (newValue == NULL) {
            NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
            long keyHash = key.hash64();
            MemoryBlock value = getValueToStore(entry);
            MemoryBlock oldValue = recordsWithNullValue.put(keyHash, key, value);
            return oldValue;
        } else {
            return mapAttributeToEntry(newValue, entry);
        }
    }

    @Override
    Object removeInternal(Comparable value, Data recordKey) {
        if (value == NULL) {
            long keyHash = recordKey.hash64();
            return recordsWithNullValue.remove(keyHash, (NativeMemoryData) recordKey);
        } else {
            return removeMappingForAttribute(value, recordKey);
        }
    }

    @Override
    public void clear() {
        recordsWithNullValue.clear();
        records.clear();
    }

    @Override
    public void destroy() {
        dispose();
    }

    @Override
    public boolean isEvaluateOnly() {
        return false;
    }

    @Override
    public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
        return false;
    }

    @Override
    public Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        return doGetRecords(value);
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        Set<QueryableEntry> results = new HashSet<>();
        for (Comparable value : values) {
            results.addAll(doGetRecords(value));
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        return buildResultSet(getRecords0(comparison, value));
    }

    private Object mapAttributeToEntry(Comparable attribute, QueryableEntry entry) {
        NativeMemoryData key = (NativeMemoryData) entry.getKeyData();
        MemoryBlock value = getValueToStore(entry);
        MemoryBlock oldValue = records.put(attribute, key, value);
        return oldValue;
    }

    private Object removeMappingForAttribute(Comparable attribute, Data indexKey) {
        MemoryBlock oldValue = records.remove(attribute, (NativeMemoryData) indexKey);
        return oldValue;
    }

    private void dispose() {
        recordsWithNullValue.dispose();
        records.dispose();
    }

    private MemoryBlock getValueToStore(QueryableEntry entry) {
        return (MemoryBlock) entry.getValueData();
    }

    private Iterator<QueryableEntry> getRecords0(Comparison comparison, Comparable value) {
        Iterator<QueryableEntry> result;
        switch (comparison) {
            case LESS:
                result = records.getKeysInRange(null, true, value, false);
                break;
            case LESS_OR_EQUAL:
                result = records.getKeysInRange(null, true, value, true);
                break;
            case GREATER:
                result = records.getKeysInRange(value, false, null, true);
                break;
            case GREATER_OR_EQUAL:
                result = records.getKeysInRange(value, true, null, true);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized comparison: " + comparison);
        }
        return result;
    }


    private Set<QueryableEntry> doGetRecords(Comparable value) {
        if (value == NULL) {
            return buildResultSet(recordsWithNullValue.getKeysInRange(null, true, null, true));
        } else {
            return buildResultSet(records.lookup(value));
        }
    }

    protected Set<QueryableEntry> buildResultSet(Iterator<QueryableEntry> it) {
        if (!it.hasNext()) {
            return Collections.emptySet();
        } else {
            Set<QueryableEntry> resultSet = new HashSet<>();
            while (it.hasNext()) {
                resultSet.add(it.next());
            }
            return resultSet;
        }
    }

}
