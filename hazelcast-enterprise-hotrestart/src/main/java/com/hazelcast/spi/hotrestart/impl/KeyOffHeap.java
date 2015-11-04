package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Implementation of {@link HotRestartKey} for off-heap hot restart store.
 */
public final class KeyOffHeap implements HotRestartKey {
    private final long prefix;
    private final byte[] bytes;
    private final SimpleHandleOffHeap handle;

    @SuppressFBWarnings(value = "EI",
            justification = "bytes is an effectively immutable array (it is illegal to change its contents)")
    public KeyOffHeap(long prefix, byte[] bytes, long address, long sequenceId) {
        this.prefix = prefix;
        this.bytes = bytes;
        this.handle = new SimpleHandleOffHeap(address, sequenceId);
    }

    @Override public KeyHandleOffHeap handle() {
        return handle;
    }

    @Override public long prefix() {
        return prefix;
    }

    @SuppressFBWarnings(value = "EI",
            justification = "bytes is an effectively immutable array (it is illegal to change its contents)")
    @Override public byte[] bytes() {
        return bytes;
    }
}
