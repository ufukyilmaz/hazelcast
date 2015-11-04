package com.hazelcast.spi.hotrestart;

/**
 * {@link KeyHandle} for an off-heap record. The key handle consists of a pair
 * {@code (address, sequenceId}: <ul>
 *     <li>{@code address} is a raw pointer to the data structure
 *     containing the key in the RAM store;</li>
 *     <li>{@code sequenceId} is a unique integer associated with the pointer. This is needed
 *     to prevent the A-B-A problem where the same pointer could be first invalidated, then
 *     later reused for an unrelated data structure.</li>
 * </ul>
 */
public interface KeyHandleOffHeap extends KeyHandle {
    /**
     * @return the address of the key in the RAM store.
     */
    long address();

    /**
     * @return the sequence ID associated with the key in the RAM store.
     */
    long sequenceId();
}
