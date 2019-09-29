package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.internal.hotrestart.KeyHandleOffHeap;

import static com.hazelcast.internal.util.HashUtil.fastLongMix;

/**
 * Simple implementation of {@link KeyHandleOffHeap}.
 */
public class SimpleHandleOffHeap implements KeyHandleOffHeap {
    private final long address;
    private final long sequenceId;

    public SimpleHandleOffHeap(long address, long sequenceId) {
        this.address = address;
        this.sequenceId = sequenceId;
    }

    @Override
    public long address() {
        return address;
    }

    @Override
    public long sequenceId() {
        return sequenceId;
    }

    @Override
    public boolean equals(Object o) {
        final KeyHandleOffHeap that;
        return this == o || o instanceof KeyHandleOffHeap
                && this.address == (that = (KeyHandleOffHeap) o).address()
                && this.sequenceId == that.sequenceId();
    }

    @Override
    public int hashCode() {
        return (int) fastLongMix(fastLongMix(address()) + sequenceId());
    }

    @Override
    public String toString() {
        return String.format("(%x,%x)", address, sequenceId);
    }
}
