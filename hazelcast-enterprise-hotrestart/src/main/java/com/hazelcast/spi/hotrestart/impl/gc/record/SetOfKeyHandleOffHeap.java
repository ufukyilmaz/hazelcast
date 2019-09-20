package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray16byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyNoValue;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;

import static com.hazelcast.internal.util.HashUtil.fastLongMix;

/**
 * Off-heap implementation of {@link SetOfKeyHandle}.
 */
public final class SetOfKeyHandleOffHeap implements SetOfKeyHandle {
    private final HashSlotArray16byteKey hsa;

    public SetOfKeyHandleOffHeap(MemoryManager memMgr) {
        this.hsa = new HashSlotArray16byteKeyNoValue(0L, memMgr);
        hsa.gotoNew();
    }

    @Override
    public void add(KeyHandle kh) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        hsa.ensure(ohk.address(), ohk.sequenceId());
    }

    @Override
    public void remove(KeyHandle kh) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        hsa.remove(ohk.address(), ohk.sequenceId());
    }

    @Override
    public KhCursor cursor() {
        return new Cursor();
    }

    @Override
    public void dispose() {
        hsa.dispose();
    }

    private final class Cursor implements KhCursor, KeyHandleOffHeap {
        private final HashSlotCursor16byteKey c = hsa.cursor();

        @Override
        public boolean advance() {
            return c.advance();
        }

        @Override
        public KeyHandleOffHeap asKeyHandle() {
            return this;
        }

        @Override
        public long address() {
            return c.key1();
        }

        @Override
        public long sequenceId() {
            return c.key2();
        }

        @Override
        public boolean equals(Object obj) {
            final KeyHandleOffHeap that;
            return obj instanceof KeyHandleOffHeap
                    && this.address() == (that = (KeyHandleOffHeap) obj).address()
                    && this.sequenceId() == that.sequenceId();
        }

        @Override
        public int hashCode() {
            return (int) fastLongMix(fastLongMix(address()) + sequenceId());
        }
    }
}
