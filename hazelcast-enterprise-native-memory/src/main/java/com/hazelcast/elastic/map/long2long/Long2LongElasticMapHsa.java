package com.hazelcast.elastic.map.long2long;

import com.hazelcast.elastic.map.hashslot.HashSlotArray;
import com.hazelcast.elastic.map.hashslot.HashSlotArrayImpl;
import com.hazelcast.elastic.map.hashslot.HashSlotCursor;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.memory.MemoryAllocator;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.util.Collections.emptySet;

/**
 * a {@link Long2LongElasticMap} implemented in terms of a {@link HashSlotArray}.
 */
public class Long2LongElasticMapHsa implements Long2LongElasticMap {

    private final HashSlotArray hsa;
    private final long nullValue;

    public Long2LongElasticMapHsa(long nullValue, MemoryAllocator malloc) {
        this.hsa = new HashSlotArrayImpl(nullValue, malloc, LONG_SIZE_IN_BYTES);
        this.nullValue = nullValue;
    }

    @Override public long get(long key) {
        final long valueAddr = hsa.get(key);
        return valueAddr != NULL_ADDRESS ? MEM.getLong(valueAddr) : nullValue;
    }

    @Override public long put(long key, long value) {
        assert value != nullValue : "put() called with null-sentinel value " + nullValue;
        long valueAddr = hsa.ensure(key);
        long result;
        if (valueAddr < 0) {
            valueAddr = -valueAddr;
            result = MEM.getLong(valueAddr);
        } else {
            result = nullValue;
        }
        MEM.putLong(valueAddr, value);
        return result;
    }

    @Override public boolean set(long key, long value) {
        assert value != nullValue : "set() called with null-sentinel value " + nullValue;
        return put(key, value) == nullValue;
    }

    @Override public long putIfAbsent(long key, long value) {
        assert value != nullValue : "putIfAbsent() called with null-sentinel value " + nullValue;
        long valueAddr = hsa.ensure(key);
        if (valueAddr > 0) {
            MEM.putLong(valueAddr, value);
            return nullValue;
        } else {
            valueAddr = -valueAddr;
            return MEM.getLong(valueAddr);
        }
    }

    @Override public boolean replace(long key, long oldValue, long newValue) {
        assert oldValue != nullValue : "replace() called with null-sentinel oldValue " + nullValue;
        assert newValue != nullValue : "replace() called with null-sentinel newValue " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return false;
        }
        final long actualValue = MEM.getLong(valueAddr);
        if (actualValue != oldValue) {
            return false;
        }
        MEM.putLong(valueAddr, newValue);
        return true;
    }

    @Override public long replace(long key, long value) {
        assert value != nullValue : "replace() called with null-sentinel value " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return nullValue;
        }
        final long oldValue = MEM.getLong(valueAddr);
        MEM.putLong(valueAddr, value);
        return oldValue;
    }

    @Override public long remove(long key) {
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return nullValue;
        }
        final long oldValue = MEM.getLong(valueAddr);
        hsa.remove(key);
        return oldValue;
    }

    @Override public boolean delete(long key) {
        return hsa.remove(key);
    }

    @Override public boolean remove(long key, long value) {
        assert value != nullValue : "remove() called with null-sentinel value " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return false;
        }
        final long actualValue = MEM.getLong(valueAddr);
        if (actualValue == value) {
            hsa.remove(key);
            return true;
        }
        return false;
    }

    @Override public boolean containsKey(long key) {
        return hsa.get(key) != NULL_ADDRESS;
    }

    @Override public boolean containsValue(long value) {
        assert value != nullValue : "containsValue() called with null-sentinel value " + nullValue;
        for (HashSlotCursor cursor = hsa.cursor(); cursor.advance();) {
            if (MEM.getLong(cursor.valueAddress()) == value) {
                return true;
            }
        }
        return false;
    }

    @Override public Long put(Long key, Long value) {
        return put((long) key, (long) value);
    }

    @Override public boolean set(Long key, Long value) {
        return set((long) key, (long) value);
    }

    @Override public Long putIfAbsent(Long key, Long value) {
        return putIfAbsent((long) key, (long) value);
    }

    @Override public boolean replace(Long key, Long oldValue, Long newValue) {
        return replace((long) key, (long) oldValue, (long) newValue);
    }

    @Override public Long replace(Long key, Long value) {
        return replace((long) key, (long) value);
    }

    @Override public Long remove(Object key) {
        return key instanceof Long ? remove((long) (Long) key) : null;
    }

    @Override public void putAll(Map<? extends Long, ? extends Long> m) {
        for (Entry<? extends Long, ? extends Long> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override public boolean delete(Long key) {
        return delete((long) key);
    }

    @Override public boolean remove(Object key, Object value) {
        return (key instanceof Long && value instanceof Long) && remove((long) (Long) key, (long) (Long) value);
    }

    @Override public Long get(Object key) {
        return key instanceof Long ? get((long) (Long) key) : null;
    }

    @Override public boolean containsKey(Object key) {
        return key instanceof Long && containsKey((long) (Long) key);
    }

    @Override public boolean containsValue(Object value) {
        return value instanceof Long && containsValue((long) (Long) value);
    }

    @Override public Set<Long> keySet() {
        hsa.get(0);
        return emptySet();
    }

    @Override public Collection<Long> values() {
        hsa.get(0);
        return emptySet();
    }

    @Override public Set<Entry<Long, Long>> entrySet() {
        hsa.get(0);
        return emptySet();
    }

    @Override public int size() {
        final long size = hsa.size();
        return (int) Math.min(Integer.MAX_VALUE, size);
    }

    @Override public boolean isEmpty() {
        return hsa.size() == 0;
    }

    @Override public void clear() {
        hsa.clear();
    }

    @Override public void dispose() {
        hsa.dispose();
    }
}
