package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.elastic.map.InlineNativeMemoryMap;
import com.hazelcast.elastic.map.InlineNativeMemoryMapImpl;
import com.hazelcast.elastic.map.InmmCursor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Off-heap implementation of record tracker map.
 */
final class TrackerMapOffHeap implements TrackerMap {
    private static final float LOAD_FACTOR = 0.6f;
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private InlineNativeMemoryMap trackers;
    private TrackerOffHeap tr = new TrackerOffHeap();

    TrackerMapOffHeap(MemoryAllocator malloc) {
        this.trackers = new InlineNativeMemoryMapImpl(malloc, TrackerOffHeap.SIZE);
    }

    @Override public Tracker putIfAbsent(KeyHandle kh, long chunkSeq, boolean isTombstone) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final long addr = trackers.put(ohk.address(), ohk.sequenceId());
        if (addr > 0) {
            tr.address = addr;
            tr.setState(chunkSeq, isTombstone);
            tr.resetGarbageCount();
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

    @Override public void remove(KeyHandle kh) {
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

    @Override public String toString() {
        final StringBuilder b = new StringBuilder(1024);
        for (CursorOffHeap c = new CursorOffHeap(); c.advance();) {
            b.append(c.address()).append(',').append(c.sequenceId()).append("->").append(c.asTracker()).append(' ');
        }
        return b.toString();
    }

    private class CursorOffHeap implements Cursor, KeyHandleOffHeap {
        private final InmmCursor c = trackers.cursor();
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
