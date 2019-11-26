package com.hazelcast.query.impl;

import com.hazelcast.internal.elastic.map.BinaryElasticHashMap;
import com.hazelcast.internal.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.nio.serialization.Data;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Wrapper around BinaryElasticHashMap (BEHM) for the usage in IndexStores
 * Does validation and necessary transformations.
 *
 * Contract:
 * - Expects the key to be in the NativeMemoryData,
 * - Expects the value to be in either NativeMemoryData or HDRecord,
 * - Returns NativeMemoryData or HDRecord,
 * - Never disposes any NativeMemoryData/HDRecord passed to it,
 * - Uses MapEntryFactory to create MapEntry instances in methods that return them.
 *
 * @param <T> type of the QueryableEntry entry passed to the map
 */
class HDIndexHashMap<T extends QueryableEntry> {

    private final MapEntryFactory<T> entryFactory;

    private final EnterpriseSerializationService ess;
    private final MemoryAllocator malloc;

    private BinaryElasticHashMap<MemoryBlock> records;
    private final HDExpirableIndexStore indexStore;

    HDIndexHashMap(HDExpirableIndexStore indexStore, EnterpriseSerializationService ess, MemoryAllocator malloc,
                   MapEntryFactory<T> entryFactory) {
        this.indexStore = indexStore;
        this.ess = ess;
        this.malloc = malloc;

        MemoryBlockAccessor valueAccessor = new NativeMemoryDataAccessor(ess);
        MemoryBlockAccessor recordAccessor = new HDRecordAccessor(ess);
        this.records = new BinaryElasticHashMap<MemoryBlock>(ess,
                new HDIndexBehmSlotAccessorFactory(),
                new HDIndexBehmMemoryBlockAccessor(valueAccessor, recordAccessor), malloc);

        this.entryFactory = entryFactory;
    }

    /**
     * Put operation
     *
     * @param keyData   must be NativeMemoryData
     * @param valueData must be NativeMemoryData
     * @return old value as NativeMemoryData/HDRecord or null
     */
    public MemoryBlock put(NativeMemoryData keyData, MemoryBlock valueData) {
        checkNotNull(keyData, "record can't be null");
        if (valueData == null) {
            valueData = new NativeMemoryData();
        }
        return records.put(keyData, valueData);
    }

    /**
     * Remove operation
     *
     * @param key can be any Data implementation (on-heap or off-heap)
     * @return old value as NativeMemoryData/HDRecord or null
     */
    public MemoryBlock remove(Data key) {
        checkNotNull(key, "key can't be null");
        return records.remove(key);
    }

    /**
     * Uses MapEntryFactory to create MapEntry instances.
     *
     * @return Returns the entrySet of all entries.
     */
    public Set<T> entrySet() {
        // here we transform the entries using the MapEntryFactory.
        Set<T> result = new HashSet<T>();
        for (Map.Entry<Data, MemoryBlock> entry : records.entrySet()) {
            Data key = entry.getKey();
            MemoryBlock memoryBlock = entry.getValue();
            NativeMemoryData valueData;
            if (memoryBlock instanceof NativeMemoryData || memoryBlock == null) {
                valueData = (NativeMemoryData) memoryBlock;
            } else {
                valueData = indexStore.getValueOrNullIfExpired(key, memoryBlock);
            }
            if (memoryBlock == null || valueData != null) {
                result.add(entryFactory.create(entry.getKey(), valueData));
            }
        }
        return result;
    }

    /**
     * Clears the map by disposing and re-initting it. Key / Value pairs aren't touched, owned externally.
     */
    public void clear() {
        records.dispose();
        MemoryBlockAccessor valueAccessor = new NativeMemoryDataAccessor(ess);
        MemoryBlockAccessor recordAccessor = new HDRecordAccessor(ess);
        records = new BinaryElasticHashMap<MemoryBlock>(ess, new HDIndexBehmSlotAccessorFactory(),
                new HDIndexBehmMemoryBlockAccessor(valueAccessor, recordAccessor), malloc);
    }

    /**
     * Disposes internal backing BEHM. Does not dispose key/value pairs inside.
     * To dispose key/value pairs, {@link #clear()} must be called explicitly.
     *
     * @see #clear()
     */
    public void dispose() {
        records.dispose();
    }

    public long size() {
        return records.size();
    }

}
