package com.hazelcast.internal.bplustree;

/**
 * Defines the entry slot payload interface.
 */
public interface EntrySlotPayload {

    /**
     * @return the payload size in bytes
     */
    int getPayloadSize();

    /**
     * Sets the payload on the given address
     * @param address the address of the payload
     * @param indexKeyAddr the index key address that is used to calculate the payload
     */
    void setPayload(long address, long indexKeyAddr);

    /**
     * Get the payload by given address
     * @param address the address of the payload
     * @return
     */
    long getPayload(long address);
}
