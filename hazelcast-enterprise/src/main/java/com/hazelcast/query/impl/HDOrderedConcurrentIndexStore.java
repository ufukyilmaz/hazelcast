package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.bplustree.BPlusTreeInlinedLongAccessor;
import com.hazelcast.internal.bplustree.BPlusTreeInlinedLongComparator;
import com.hazelcast.internal.bplustree.DefaultBPlusTreeKeyAccessor;
import com.hazelcast.internal.bplustree.DefaultBPlusTreeKeyComparator;
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

/**
 * Ordered and concurrent index store for HD memory based on B+tree.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key to be NativeMemoryData and the value
 * to be in either NativeMemoryData or HDRecord
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Iterator never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - The index operations are thread-safe and can be accessed from multiple threads concurrently
 */
public class HDOrderedConcurrentIndexStore extends BaseSingleValueIndexStore {

    private final HDBPlusTreeIndex<QueryableEntry> recordsWithNullValue;
    private final HDBPlusTreeIndex<QueryableEntry> records;

    HDOrderedConcurrentIndexStore(IndexCopyBehavior copyBehavior,
                                  EnterpriseSerializationService ess,
                                  MemoryAllocator keyAllocator,
                                  MemoryAllocator indexAllocator,
                                  MapEntryFactory<QueryableEntry> entryFactory,
                                  int nodeSize) {
        // HD index does not do any result set copying, thus we may pass NEVER here
        super(copyBehavior, false);

        this.recordsWithNullValue = new HDBPlusTreeIndex(ess, keyAllocator, indexAllocator, entryFactory,
                new BPlusTreeInlinedLongComparator(), new BPlusTreeInlinedLongAccessor(ess), nodeSize);
        try {
            this.records = new HDBPlusTreeIndex(ess, keyAllocator, indexAllocator, entryFactory,
                    new DefaultBPlusTreeKeyComparator(ess), new DefaultBPlusTreeKeyAccessor(ess), nodeSize);
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
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        // We still need to canonicalize query arguments for ordered indexes to
        // support InPredicate queries.
        return Comparables.canonicalizeForHashLookup(value);
    }

    @Override
    public Comparable canonicalizeScalarForStorage(Comparable value) {
        // Returning the original value since ordered indexes are not supporting
        // hash lookups on their stored values, so there is no need in providing
        // canonical representations.
        return value;
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

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        return buildResultSet(getRecords0(from, fromInclusive, to, toInclusive));
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

    private Iterator<QueryableEntry> getRecords0(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        return records.getKeysInRange(from, fromInclusive, to, toInclusive);
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

    private Set<QueryableEntry> buildResultSet(Iterator<QueryableEntry> it) {
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
