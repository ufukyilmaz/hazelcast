package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.elastic.map.hashslot.HashSlotArray;
import com.hazelcast.elastic.map.hashslot.HashSlotArrayImpl;
import com.hazelcast.elastic.map.hashslot.HashSlotCursor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Off-heap implementation of record tracker map.
 */
final class TrackerMapOffHeap extends TrackerMapBase {
    private static final float LOAD_FACTOR = 0.6f;
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private HashSlotArray trackers;
    private TrackerOffHeap tr = new TrackerOffHeap();

    TrackerMapOffHeap(MemoryAllocator malloc) {
        this.trackers = new HashSlotArrayImpl(malloc, TrackerOffHeap.SIZE);
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
        private final HashSlotCursor c = trackers.cursor();
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

        @Override public void remove() {
            c.remove();
        }

        @Override public long address() {
            return c.key1();
        }

        @Override public long sequenceId() {
            return c.key2();
        }
    }
}
