package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;

/** Set of key handles. Offers minimum support needed to track tombstone keys during restart. */
public interface SetOfKeyHandle extends Disposable {
    /** Adds a key handle to the set. */
    void add(KeyHandle kh);

    /** Removes a key handle from the set. */
    void remove(KeyHandle kh);

    /** @return a cursor over all key handles in the set */
    KhCursor cursor();

    interface KhCursor {

        /**
         * Attempts to advance the cursor to the next position.
         * @return {@code true} if the cursor advanced; {@code false} otherwise.
         */
        boolean advance();

        /**
         * Returns the key handle at the current cursor position. May return the cursor object itself,
         * therefore the returned object is valid only until the cursor is updated by calling {@link #advance()}
         * or the owning set is updated.
         */
        KeyHandle asKeyHandle();
    }
}
