package com.hazelcast.query.impl;

import com.hazelcast.elastic.map.BinaryElasticHashMap;
import com.hazelcast.elastic.map.NativeMemoryDataAccessor;
import com.hazelcast.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Wrapper around BinaryElasticHashMap (BEHM) for the usage in IndexStores
 * Does validation and necessary transformations.
 *
 * Contract:
 * - Expects the key & value to be in the NativeMemoryData,
 * - Returns NativeMemoryData,
 * - Never disposes any NativeMemoryData passed to it,
 * - Uses MapEntryFactory to create MapEntry instances in methods that return them.
 *
 * @param <T> type of the QueryableEntry entry passed to the map
 */
class HDIndexHashMap<T extends QueryableEntry> {

    private final BinaryElasticHashMap<NativeMemoryData> records;
    private final MapEntryFactory<T> entryFactory;

    HDIndexHashMap(EnterpriseSerializationService ess, MemoryAllocator malloc, MapEntryFactory<T> entryFactory) {
        this.records = new BinaryElasticHashMap<NativeMemoryData>(ess, new NativeMemoryDataAccessor(ess), malloc);
        this.entryFactory = entryFactory;
    }

    /**
     * Put operation
     *
     * @param keyData   must be NativeMemoryData
     * @param valueData must be NativeMemoryData
     * @return old value as NativeMemoryData or null
     */
    public NativeMemoryData put(NativeMemoryData keyData, NativeMemoryData valueData) {
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
     * @return old value as NativeMemoryData or null
     */
    public NativeMemoryData remove(Data key) {
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
        for (Map.Entry<Data, NativeMemoryData> entry : records.entrySet()) {
            result.add(entryFactory.create(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * Clears the map by removing and disposing all key/value pairs stored in the backing BEHM.
     * Does not dispose the backing BEHM.
     */
    public void clear() {
        records.clear();
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
