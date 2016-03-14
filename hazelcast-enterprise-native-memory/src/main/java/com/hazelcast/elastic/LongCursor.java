package com.hazelcast.elastic;

/**
 * Cursor over a collection of {@code long} values. Initially the cursor's location is
 * before the first element and the cursor is invalid. The cursor becomes invalid again after a
 * call to {@link #advance()} return {@code false}. It is illegal to call any methods except
 * {@link #reset} on an invalid cursor.
 */
public interface LongCursor {

    /**
     * Advances to the next {@code long} element. It is illegal to call this method after a previous
     * call returned {@code false}. An {@code AssertionError} may be thrown.
     *
     * @return {@code true} if the cursor advanced. If {@code false} is returned, the cursor is now invalid.
     */
    boolean advance();

    /**
     * @return the {@code long} value at the cursor's current position
     */
    long value();

    /**
     * Resets the cursor to the inital state.
     */
    void reset();
}
