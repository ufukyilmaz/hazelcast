package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.elastic.map.HashSlotArray;
import com.hazelcast.elastic.map.HashSlotArrayImpl;
import com.hazelcast.elastic.map.HashSlotCursor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;

final class SetOfKeyHandleOffHeap implements SetOfKeyHandle {
    private final HashSlotArray set;

    SetOfKeyHandleOffHeap(MemoryAllocator malloc) {
        this.set = new HashSlotArrayImpl(malloc, 0);
    }

    @Override public void add(KeyHandle kh) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        set.ensure(ohk.address(), ohk.sequenceId());
    }

    @Override public void remove(KeyHandle kh) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        set.remove(ohk.address(), ohk.sequenceId());
    }

    @Override public KhCursor cursor() {
        return new Cursor();
    }

    @Override public void dispose() {
        set.dispose();
    }

    private final class Cursor implements KhCursor, KeyHandleOffHeap {
        private final HashSlotCursor c = set.cursor();

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
    }
}
