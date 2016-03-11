package com.hazelcast.spi.hotrestart.impl.gc.tracker;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.spi.hashslot.HashSlotArrayTwinKey;
import com.hazelcast.spi.hashslot.HashSlotArrayTwinKeyImpl;
import com.hazelcast.spi.hashslot.HashSlotCursorTwinKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.spi.hashslot.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * Off-heap implementation of record tracker map.
 */
public final class TrackerMapOffHeap extends TrackerMapBase {

    @SuppressWarnings("checkstyle:magicnumber")
    private static final int INITIAL_CAPACITY = 1 << 9;
    private HashSlotArrayTwinKey trackers;
    private TrackerOffHeap tr = new TrackerOffHeap();

    public TrackerMapOffHeap(MemoryManager memMgr, MemoryAllocator auxMalloc) {
        this.trackers = new HashSlotArrayTwinKeyImpl(
                Long.MIN_VALUE, memMgr, auxMalloc, TrackerOffHeap.SIZE, INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
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
        private final HashSlotCursorTwinKey c = trackers.cursor();
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
