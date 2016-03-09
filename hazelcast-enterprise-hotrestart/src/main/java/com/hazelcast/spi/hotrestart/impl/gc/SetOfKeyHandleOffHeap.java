package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hashslot.HashSlotArrayTwinKey;
import com.hazelcast.spi.hashslot.HashSlotArrayTwinKeyNoValue;
import com.hazelcast.spi.hashslot.HashSlotCursorTwinKey;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryManagerBean;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.util.HashUtil.fastLongMix;

final class SetOfKeyHandleOffHeap implements SetOfKeyHandle {
    private final HashSlotArrayTwinKey hsa;

    SetOfKeyHandleOffHeap(MemoryAllocator malloc) {
        this.hsa = new HashSlotArrayTwinKeyNoValue(0L, new MemoryManagerBean(malloc, AMEM));
        hsa.gotoNew();
    }

    @Override public void add(KeyHandle kh) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        hsa.ensure(ohk.address(), ohk.sequenceId());
    }

    @Override public void remove(KeyHandle kh) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        hsa.remove(ohk.address(), ohk.sequenceId());
    }

    @Override public KhCursor cursor() {
        return new Cursor();
    }

    @Override public void dispose() {
        hsa.dispose();
    }

    private final class Cursor implements KhCursor, KeyHandleOffHeap {
        private final HashSlotCursorTwinKey c = hsa.cursor();

        @Override public boolean advance() {
            return c.advance();
        }

        @Override public KeyHandleOffHeap asKeyHandle() {
            return this;
        }

        @Override public long address() {
            return c.key1();
        }

        @Override public long sequenceId() {
            return c.key2();
        }

        @Override public boolean equals(Object obj) {
            final KeyHandleOffHeap that;
            return obj instanceof KeyHandleOffHeap
                    && this.address() == (that = (KeyHandleOffHeap) obj).address()
                    && this.sequenceId() == that.sequenceId();
        }

        @Override public int hashCode() {
            return (int) fastLongMix(fastLongMix(address()) + sequenceId());
        }
    }
}
