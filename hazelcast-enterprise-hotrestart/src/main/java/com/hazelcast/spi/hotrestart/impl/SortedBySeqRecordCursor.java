package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;

/**
 * Cursor over records which are sorted by their {@code recordSeq}.
 */
public interface SortedBySeqRecordCursor extends Disposable {

    boolean advance();

    KeyHandle asKeyHandle();

    Record asRecord();
}
