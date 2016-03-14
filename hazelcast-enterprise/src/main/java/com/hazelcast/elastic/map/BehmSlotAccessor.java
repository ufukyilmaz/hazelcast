package com.hazelcast.elastic.map;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.util.HashUtil.MurmurHash3_fmix;

/** Helper class with logic to access BinaryElasticHashMap's slots */
public class BehmSlotAccessor {
    /** An slot consists of a key pointer (8 bytes) and a value pointer (8 bytes) */
    static final long SLOT_LENGTH = 16L;
    /** Location of the key pointer in a slot */
    static final int KEY_OFFSET = 0;
    /** Location of the value pointer in a slot */
    static final int VALUE_OFFSET = 8;

    private final MemoryAllocator malloc;
    private long baseAddr;
    private long size;

    BehmSlotAccessor(MemoryAllocator malloc) {
        this.malloc = malloc;
    }

    BehmSlotAccessor(BehmSlotAccessor that) {
        this.malloc = that.malloc;
        this.baseAddr = that.baseAddr;
        this.size = that.size;
    }

    static int rehash(Data o, int p) {
        return (o == null) ? 0 : MurmurHash3_fmix(o.hashCode() ^ p);
    }

    static int rehash(int v, int p) {
        return MurmurHash3_fmix(v ^ p);
    }

    BehmSlotAccessor allocate(int slotCapacity) {
        this.size = slotCapacity * SLOT_LENGTH;
        this.baseAddr = malloc.allocate(size);
        return this;
    }

    void clear() {
        AMEM.setMemory(baseAddr, size, (byte) 0);
    }

    void delete() {
        malloc.free(baseAddr, size);
        baseAddr = NULL_ADDRESS;
        size = 0;
    }

    public boolean isAssigned(int slot) {
        return getKey(slot) != 0L;
    }

    public long getKey(int slot) {
        return AMEM.getLong(slotBase(slot) + KEY_OFFSET);
    }

    public void setKey(int slot, long key) {
        AMEM.putLong(slotBase(slot) + KEY_OFFSET, key);
    }

    public long getValue(int slot) {
        return AMEM.getLong(slotBase(slot) + VALUE_OFFSET);
    }

    public void setValue(int slot, long value) {
        AMEM.putLong(slotBase(slot) + VALUE_OFFSET, value);
    }

    public NativeMemoryData keyData(int slot) {
        final long keyAddr = getKey(slot);
        if (keyAddr != NULL_ADDRESS) {
            return new NativeMemoryData().reset(keyAddr);
        }
        return null;
    }

    private long slotBase(int slot) {
        return baseAddr + slot * SLOT_LENGTH;
    }

    @Override public String toString() {
        return String.valueOf(baseAddr);
    }
}
