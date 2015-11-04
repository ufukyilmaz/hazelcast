package com.hazelcast.elastic.map;

/**
 * Inline native memory map cursor. Initially the cursor's location is
 * before the first map entry and the cursor is invalid.
 */
public interface InmmCursor {
    /**
     * Advance to the next map entry.
     * @return true if the cursor has advanced. If false is returned, the cursor is now invalid.
     * @throws IllegalStateException if a previous call to advance() already returned false.
     */
    boolean advance();

    /**
     * @return key part 1 of current entry.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key1();

    /**
     * @return key part 2 of current entry.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key2();

    /**
     * @return Address of the current entry's value block.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long valueAddress();

    void remove();
}
