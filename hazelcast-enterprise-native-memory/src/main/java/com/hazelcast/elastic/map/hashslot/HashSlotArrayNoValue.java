package com.hazelcast.elastic.map.hashslot;

import com.hazelcast.memory.MemoryAllocator;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;

/**
 * Specialization of {@link HashSlotArrayImpl} to the case of zero-length value. Suitable for a {@code long} set
 * implementation. Unassigned sentinel is kept at the start of the slot, i.e., in the key part.
 * Therefore the sentinel value cannot be used as a key.
 */
public class HashSlotArrayNoValue extends HashSlotArrayImpl {

    public HashSlotArrayNoValue(long unassignedSentinel, MemoryAllocator malloc, int valueLength, int initialCapacity) {
        super(unassignedSentinel, 0L, malloc, valueLength, initialCapacity);
    }

    public HashSlotArrayNoValue(long unassignedSentinel, MemoryAllocator malloc, int valueLength) {
        super(unassignedSentinel, 0L, malloc, valueLength, DEFAULT_CAPACITY);
    }
}
