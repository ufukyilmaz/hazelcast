package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;

/**
 * Cursor over records sorted by their {@code recordSeq}. A newly obtained cursor is positioned
 * before the first record.
 */
public interface SortedBySeqRecordCursor extends Disposable {

    /**
     * Attempts to advance the cursor to the next position.
     * @return {@code true} if the cursor advanced; {@code false} otherwise.
     */
    boolean advance();

    /**
     * Returns the key handle associated with the record at the current cursor position. May return
     * the cursor object itself, therefore the returned object is valid only until the cursor is
     * updated by calling {@link #advance()}.
     */
    KeyHandle asKeyHandle();

    /**
     * Returns the record at the current cursor position. May return the cursor object itself,
     * therefore the returned object is valid only until the cursor is updated by calling {@link #advance()}.
     */
    Record asRecord();
}
