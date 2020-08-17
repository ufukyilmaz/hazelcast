package com.hazelcast.internal.bplustree;

/**
 * Entry slot has no payload.
 */
public class EntrySlotNoPayload implements EntrySlotPayload {

    @Override
    public int getPayloadSize() {
        return 0;
    }

    @Override
    public void setPayload(long address, long indexKeyAddr) {
       // no-op
    }

    @Override
    public long getPayload(long address) {
        return 0;
    }
}
