package com.hazelcast.elastic.map.long2long;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.Bits;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;
import static com.hazelcast.util.HashUtil.MurmurHash3_fmix;

/**
 * Helper class with logic to access {@link Long2LongElasticHashMap}'s slots.
 */
class Long2LongSlotAccessor {

    /** An entry consists only a long key (8 bytes) and a long value (8 bytes) */
    static final long ENTRY_LENGTH = 16L;
    /** Position of key pointer in an entry */
    static final int KEY_OFFSET = 0;
    /** Position of value pointer in an entry */
    static final int VALUE_OFFSET = 8;

    private final MemoryAllocator malloc;
    private final long missingValue;
    private long baseAddr;
    private long size;

    Long2LongSlotAccessor(MemoryAllocator malloc, long missingValue) {
        this.malloc = malloc;
        this.missingValue = missingValue;
    }

    Long2LongSlotAccessor(Long2LongSlotAccessor that) {
        this.malloc = that.malloc;
        this.missingValue = that.missingValue;
        this.baseAddr = that.baseAddr;
        this.size = that.size;
    }

    static int rehash(long v, int p) {
        return (int) MurmurHash3_fmix(v ^ p);
    }

    Long2LongSlotAccessor allocate(int slotCapacity) {
        this.size = slotCapacity * ENTRY_LENGTH;
        this.baseAddr = malloc.allocate(size);
        clear();
        return this;
    }

    void clear() {
        for (long i = 0; i < size; i += ENTRY_LENGTH) {
            UNSAFE.putLong(baseAddr + i, missingValue);
            UNSAFE.putLong(baseAddr + i + Bits.LONG_SIZE_IN_BYTES, missingValue);
        }
    }

    void delete() {
        malloc.free(baseAddr, size);
        baseAddr = NULL_ADDRESS;
        size = 0;
    }

    boolean isAssigned(int slot) {
        return getKey(slot) != missingValue;
    }

    long getKey(int slot) {
        return UNSAFE.getLong(slotBase(slot) + KEY_OFFSET);
    }

    void setKey(int slot, long key) {
        UNSAFE.putLong(slotBase(slot) + KEY_OFFSET, key);
    }

    long getValue(int slot) {
        return UNSAFE.getLong(slotBase(slot) + VALUE_OFFSET);
    }

    void setValue(int slot, long value) {
        UNSAFE.putLong(slotBase(slot) + VALUE_OFFSET, value);
    }

    long slotBase(int slot) {
        return baseAddr + slot * ENTRY_LENGTH;
    }

}
