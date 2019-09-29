package com.hazelcast.internal.hotrestart.impl.gc.tracker;

import com.hazelcast.internal.hotrestart.KeyHandle;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * On-heap implementation of record tracker map.
 */
public final class TrackerMapOnHeap extends TrackerMapBase {
    private final Map<KeyHandle, Tracker> trackers = new HashMap<KeyHandle, Tracker>();

    @Override
    public Tracker putIfAbsent(KeyHandle kh, long chunkSeq, boolean isTombstone) {
        final Tracker tr = trackers.get(kh);
        if (tr != null) {
            return tr;
        }
        trackers.put(kh, new TrackerOnHeap(chunkSeq, isTombstone));
        added(isTombstone);
        return null;
    }

    @Override
    public Tracker get(KeyHandle kh) {
        return trackers.get(kh);
    }

    @Override
    void doRemove(KeyHandle kh) {
        trackers.remove(kh);
    }

    @Override
    public long size() {
        return trackers.size();
    }

    @Override
    public Cursor cursor() {
        return new HeapCursor();
    }

    @Override
    public void dispose() { }

    private class HeapCursor implements Cursor {
        private final Iterator<Entry<KeyHandle, Tracker>> iter = trackers.entrySet().iterator();
        private Entry<KeyHandle, Tracker> current;

        @Override
        public boolean advance() {
            if (!iter.hasNext()) {
                return false;
            }
            current = iter.next();
            return true;
        }

        @Override
        public KeyHandle asKeyHandle() {
            return toKeyHandle();
        }

        @Override
        public KeyHandle toKeyHandle() {
            return current.getKey();
        }

        @Override
        public Tracker asTracker() {
            return current.getValue();
        }
    }
}
