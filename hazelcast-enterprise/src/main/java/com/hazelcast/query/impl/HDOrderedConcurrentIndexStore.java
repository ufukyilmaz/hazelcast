package com.hazelcast.query.impl;

import com.hazelcast.internal.bplustree.DefaultBPlusTreeKeyComparator;
import com.hazelcast.internal.bplustree.EntrySlotNoPayload;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.util.Set;

/**
 * Ordered and concurrent index store for HD memory based on B+tree.
 * <p>
 * Contract:
 * - Whenever QueryableEntry is passed to it, expects the key to be NativeMemoryData and the value
 * to be in either NativeMemoryData
 * - Whenever Data is passed to it (removeInternal), expects it to be NativeMemoryData
 * - Iterator never returns any native memory - all returning objects are on-heap (QueryableEntry and its fields).
 * - The index operations are thread-safe and can be accessed from multiple threads concurrently
 */
@SuppressWarnings("rawtypes")
public class HDOrderedConcurrentIndexStore extends HDBaseConcurrentIndexStore {

    HDOrderedConcurrentIndexStore(IndexCopyBehavior copyBehavior,
                                  EnterpriseSerializationService ess,
                                  MemoryAllocator keyAllocator,
                                  MemoryAllocator indexAllocator,
                                  MapEntryFactory<QueryableEntry> entryFactory,
                                  int nodeSize) {
        super(copyBehavior,
                ess,
                keyAllocator,
                indexAllocator,
                new DefaultBPlusTreeKeyComparator(ess),
                entryFactory,
                nodeSize,
                new EntrySlotNoPayload());
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        return buildResultSet(getRecords0(from, fromInclusive, to, toInclusive));
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
}
