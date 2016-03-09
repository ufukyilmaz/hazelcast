package com.hazelcast.elastic.map.long2long;

import com.hazelcast.spi.hashslot.HashSlotArray;
import com.hazelcast.spi.hashslot.HashSlotArrayImpl;
import com.hazelcast.spi.hashslot.HashSlotCursor;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.memory.MemoryManager;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * a {@link Long2LongElasticMap} implemented in terms of a {@link HashSlotArray}.
 */
public class Long2LongElasticMapHsa implements Long2LongElasticMap {

    private final HashSlotArray hsa;
    private final long nullValue;
    private MemoryAccessor mem;

    public Long2LongElasticMapHsa(long nullValue, MemoryManager memMgr) {
        this.hsa = new HashSlotArrayImpl(nullValue, memMgr, LONG_SIZE_IN_BYTES);
        this.mem = memMgr.getAccessor();
        this.nullValue = nullValue;
    }

    @Override public long get(long key) {
        final long valueAddr = hsa.get(key);
        return valueAddr != NULL_ADDRESS ? mem.getLong(valueAddr) : nullValue;
    }

    @Override public long put(long key, long value) {
        assert value != nullValue : "put() called with null-sentinel value " + nullValue;
        long valueAddr = hsa.ensure(key);
        long result;
        if (valueAddr < 0) {
            valueAddr = -valueAddr;
            result = mem.getLong(valueAddr);
        } else {
            result = nullValue;
        }
        mem.putLong(valueAddr, value);
        return result;
    }

    @Override public long putIfAbsent(long key, long value) {
        assert value != nullValue : "putIfAbsent() called with null-sentinel value " + nullValue;
        long valueAddr = hsa.ensure(key);
        if (valueAddr > 0) {
            mem.putLong(valueAddr, value);
            return nullValue;
        } else {
            valueAddr = -valueAddr;
            return mem.getLong(valueAddr);
        }
    }

    @Override public void putAll(Long2LongElasticMap from) {
        for (LongLongCursor cursor = from.cursor(); cursor.advance();) {
            put(cursor.key(), cursor.value());
        }
    }

    @Override public boolean replace(long key, long oldValue, long newValue) {
        assert oldValue != nullValue : "replace() called with null-sentinel oldValue " + nullValue;
        assert newValue != nullValue : "replace() called with null-sentinel newValue " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return false;
        }
        final long actualValue = mem.getLong(valueAddr);
        if (actualValue != oldValue) {
            return false;
        }
        mem.putLong(valueAddr, newValue);
        return true;
    }

    @Override public long replace(long key, long value) {
        assert value != nullValue : "replace() called with null-sentinel value " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return nullValue;
        }
        final long oldValue = mem.getLong(valueAddr);
        mem.putLong(valueAddr, value);
        return oldValue;
    }

    @Override public long remove(long key) {
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return nullValue;
        }
        final long oldValue = mem.getLong(valueAddr);
        hsa.remove(key);
        return oldValue;
    }

    @Override public boolean remove(long key, long value) {
        assert value != nullValue : "remove() called with null-sentinel value " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return false;
        }
        final long actualValue = mem.getLong(valueAddr);
        if (actualValue == value) {
            hsa.remove(key);
            return true;
        }
        return false;
    }

    @Override public boolean containsKey(long key) {
        return hsa.get(key) != NULL_ADDRESS;
    }

    @Override public long size() {
        return hsa.size();
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

    @Override public LongLongCursor cursor() {
        return new Cursor(hsa);
    }

    private final class Cursor implements LongLongCursor {

        private final HashSlotCursor cursor;

        Cursor(HashSlotArray hsa) {
            this.cursor = hsa.cursor();
        }

        @Override public boolean advance() {
            return cursor.advance();
        }

        @Override public long key() {
            return cursor.key();
        }

        @Override public long value() {
            return mem.getLong(cursor.valueAddress());
        }
    }
}
