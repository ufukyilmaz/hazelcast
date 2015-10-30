package com.hazelcast.elastic.map;

import com.hazelcast.nio.Disposable;

/**
 * <p>A hashtable which keeps densely packed (key, value) entries in a native memory block.
 * As a consequence, both key and value have fixed size. The key may be up to 16 bytes
 * and is represented as a pair of <code>long</code> values.
 *
 * <p>This map responds to lookup with a <code>long</code> value representing the address of
 * the value block. The actual data must be accessed using <code>Unsafe</code> memory operations.
 * <b>The returned address is valid only up to the next map update operation</b>. 
 * 
 * <p>One recommended approach to a convenient usage of the returned address is to construct 
 * a flyweight object around it and access the value's components through the object's API.
 *
 * <p>This map makes no assumptions about the key's semantics; it just uses them as the literal
 * 16 bytes of data. However, a major use case is where the key represents a "safe pointer" to
 * a native memory block. It consists of a pair <code>(nativeAddress, sequenceId)</code>, where
 * <code>nativeAddress</code> is the address of a native memory block containing key data and
 * <code>sequenceId</code> is a number unique to the particular allocation event of that memory block.
 * This avoids the "ABA problem": the block at <code>nativeAddress</code> could be deallocated, then
 * reallocated for another purpose. Then <code>nativeAddress</code> would represent a valid pointer
 * pointing to invalid data. The <code>(nativeAddress, sequenceId)</code> pair is expected to be
 * acquired from a native data structure which complies with this concept and stores the relevant
 * key data.
 */
public interface InlineNativeMemoryMap extends Disposable {

    /**
     * Ensures that there is a mapping from {@code (key1, key2)} to a slot in the map.
     * The {@code abs} of the returned integer is the address of the slot's value block.
     * The returned integer is positive if a new slot had to be mapped and negative
     * if the slot was already mapped.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return address of value block
     */
    long put(long key1, long key2);

    /**
     * Returns the address of the value block mapped by {@code (key1, key2)}.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return address of the value block or {@link com.hazelcast.memory.MemoryAllocator#NULL_ADDRESS}
     * if no mapping for {@code (key1, key2)} exists.
     */
    long get(long key1, long key2);

    /**
     * Removes the mapping for {@code (key1, key2)}.
     *
     * @param key1 key part 1
     * @param key2 key part 2
     * @return true if an entry was removed, false otherwise
     */
    boolean remove(long key1, long key2);

    /**
     * Returns the current size of the map.
     */
    long size();

    /**
     * Clears the map.
     */
    void clear();

    /**
     * Returns key length in bytes (up to 16).
     */
    int keyLength();

    /**
     * Returns value length in bytes.
     */
    int valueLength();

    InmmCursor cursor();
}
