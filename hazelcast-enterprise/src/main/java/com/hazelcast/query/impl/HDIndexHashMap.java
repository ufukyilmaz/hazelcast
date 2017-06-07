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

import static com.hazelcast.nio.serialization.DataType.HEAP;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Expects the value to be in the NativeMemoryData format
 * Never disposes any NativeMemoryData passed to it.
 *
 * @param <T> type of the QueryableEntry entry passed to the map
 */
class HDIndexHashMap<T extends QueryableEntry> {

    private final BinaryElasticHashMap<NativeMemoryData> records;
    private final MapEntryFactory<T> entryFactory;
    private final EnterpriseSerializationService ess;

    HDIndexHashMap(EnterpriseSerializationService ess, MemoryAllocator malloc, MapEntryFactory<T> entryFactory) {
        this.ess = ess;
        this.records = new BinaryElasticHashMap<NativeMemoryData>(ess, new NativeMemoryDataAccessor(ess), malloc);
        this.entryFactory = entryFactory;
    }

    public NativeMemoryData put(NativeMemoryData keyData, NativeMemoryData valueData) {
        checkNotNull(keyData, "record can't be null");
        if (valueData == null) {
            valueData = new NativeMemoryData();
        }
        return records.put(keyData, valueData);
    }

    public NativeMemoryData remove(Data key) {
        checkNotNull(key, "key can't be null");
        return records.remove(key);
    }

    /**
     * @return the on-heap representation of the entries.
     */
    public Set<T> entrySet() {
        Set<T> result = new HashSet<T>();
        for (Map.Entry<Data, NativeMemoryData> entry : records.entrySet()) {
            Data key = ess.toData(entry.getKey(), HEAP);
            Data value = ess.toData(entry.getValue(), HEAP);
            result.add(entryFactory.create(key, value));
        }
        return result;
    }

    public void clear() {
        records.clear();
    }

    public void dispose() {
        records.dispose();
    }

    public long size() {
        return records.size();
    }

}
