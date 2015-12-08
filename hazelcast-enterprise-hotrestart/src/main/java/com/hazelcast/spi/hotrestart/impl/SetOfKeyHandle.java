package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;

/** Set of key handles. Offers minimum support needed to track tombstone
 * keys during restart. */
public interface SetOfKeyHandle extends Disposable {
    void add(KeyHandle kh);

    void remove(KeyHandle kh);

    KhCursor cursor();

    interface KhCursor {
        boolean advance();

        KeyHandle asKeyHandle();
    }
}
