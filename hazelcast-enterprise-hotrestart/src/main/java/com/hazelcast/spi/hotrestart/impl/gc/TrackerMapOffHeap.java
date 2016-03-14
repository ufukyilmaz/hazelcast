package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.spi.hashslot.HashSlotArray16byteKey;
import com.hazelcast.spi.impl.hashslot.HashSlotArray16byteKeyImpl;
import com.hazelcast.spi.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Off-heap implementation of record tracker map.
 */
final class TrackerMapOffHeap extends TrackerMapBase {

    private HashSlotArray16byteKey trackers;
    private TrackerOffHeap tr = new TrackerOffHeap();

    TrackerMapOffHeap(MemoryManager memMgr) {
        this.trackers = new HashSlotArray16byteKeyImpl(Long.MIN_VALUE, memMgr, TrackerOffHeap.SIZE);
        trackers.gotoNew();
    }

    @Override public Tracker putIfAbsent(KeyHandle kh, long chunkSeq, boolean isTombstone) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final long addr = trackers.ensure(ohk.address(), ohk.sequenceId());
        if (addr > 0) {
            tr.address = addr;
            tr.setLiveState(chunkSeq, isTombstone);
            tr.resetGarbageCount();
            added(isTombstone);
            return null;
        } else {
            tr.address = -addr;
            return tr;
        }
    }

    @Override public Tracker get(KeyHandle kh) {
        final KeyHandleOffHeap handle = (KeyHandleOffHeap) kh;
        final long addr = trackers.get(handle.address(), handle.sequenceId());
        if (addr == NULL_ADDRESS) {
            return null;
        }
        tr.address = addr;
        return tr;
    }

    @Override void doRemove(KeyHandle kh) {
        final KeyHandleOffHeap handle = (KeyHandleOffHeap) kh;
        trackers.remove(handle.address(), handle.sequenceId());
    }

    @Override public long size() {
        return trackers.size();
    }

    @Override public Cursor cursor() {
        return new CursorOffHeap();
    }

    @Override public void dispose() {
        trackers.dispose();
    }

    private class CursorOffHeap implements Cursor, KeyHandleOffHeap {
        private final HashSlotCursor16byteKey c = trackers.cursor();
        private final TrackerOffHeap r = new TrackerOffHeap();

        @Override public boolean advance() {
            if (!c.advance()) {
                return false;
            }
            r.address = c.valueAddress();
            return true;
        }

        @Override public KeyHandleOffHeap asKeyHandle() {
            return this;
        }

        @Override public KeyHandle toKeyHandle() {
            return new SimpleHandleOffHeap(address(), sequenceId());
        }

        @Override public Tracker asTracker() {
            return r;
        }

        @Override public long address() {
            return c.key1();
        }

        @Override public long sequenceId() {
            return c.key2();
        }
    }
}
