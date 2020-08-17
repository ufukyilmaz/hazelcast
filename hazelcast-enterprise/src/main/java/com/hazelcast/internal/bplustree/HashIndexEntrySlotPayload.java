package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * The HASH index payload accessor. Stores index key hash value in the payload.
 */
public class HashIndexEntrySlotPayload implements EntrySlotPayload {

    @Override
    public int getPayloadSize() {
        return LONG_SIZE_IN_BYTES;
    }

    @Override
    public void setPayload(long address, long indexKeyAddr) {
        NativeMemoryData key = new NativeMemoryData().reset(indexKeyAddr);
        long indexKeyHash = key.hash64();
        AMEM.putLong(address, indexKeyHash);
    }

    @Override
    public long getPayload(long address) {
        return AMEM.getLong(address);
    }
}
