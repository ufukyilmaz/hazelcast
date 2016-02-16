package com.hazelcast.elastic.map.long2long;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAccessorProvider;
import com.hazelcast.internal.memory.MemoryAccessorType;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.Bits;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.util.HashUtil.MurmurHash3_fmix;

/**
 * Helper class with logic to access {@link Long2LongElasticHashMap}'s slots.
 */
class Long2LongSlotAccessor {

    // We are using `STANDARD` memory accessor because we internally guarantee that
    // every memory access is aligned.
    static final MemoryAccessor MEMORY_ACCESSOR =
            MemoryAccessorProvider.getMemoryAccessor(MemoryAccessorType.STANDARD);

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
            MEMORY_ACCESSOR.putLong(baseAddr + i, missingValue);
            MEMORY_ACCESSOR.putLong(baseAddr + i + Bits.LONG_SIZE_IN_BYTES, missingValue);
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
        return MEMORY_ACCESSOR.getLong(slotBase(slot) + KEY_OFFSET);
    }

    void setKey(int slot, long key) {
        MEMORY_ACCESSOR.putLong(slotBase(slot) + KEY_OFFSET, key);
    }

    long getValue(int slot) {
        return MEMORY_ACCESSOR.getLong(slotBase(slot) + VALUE_OFFSET);
    }

    void setValue(int slot, long value) {
        MEMORY_ACCESSOR.putLong(slotBase(slot) + VALUE_OFFSET, value);
    }

    long slotBase(int slot) {
        return baseAddr + slot * ENTRY_LENGTH;
    }

}
