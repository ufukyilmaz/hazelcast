package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * On-heap implementation of record tracker map.
 */
final class TrackerMapOnHeap implements TrackerMap {
    private final Map<KeyHandle, Tracker> trackers = new HashMap<KeyHandle, Tracker>();

    @Override public Tracker putIfAbsent(KeyHandle kh, long chunkSeq, boolean isTombstone) {
        final Tracker tr = trackers.get(kh);
        if (tr != null) {
            return tr;
        }
        trackers.put(kh, new TrackerOnHeap(chunkSeq, isTombstone));
        return null;
    }

    @Override public Tracker get(KeyHandle kh) {
        return trackers.get(kh);
    }

    @Override public void remove(KeyHandle kh) {
        trackers.remove(kh);
    }

    @Override public long size() {
        return trackers.size();
    }

    @Override public Cursor cursor() {
        return new HeapCursor();
    }

    @Override public void dispose() { }

    @Override public String toString() {
        final StringBuilder b = new StringBuilder(1024);
        for (Cursor c = cursor(); c.advance();) {
            b.append(c.asKeyHandle()).append("->").append(c.asTracker()).append(' ');
        }
        return b.toString();
    }

    private class HeapCursor implements Cursor {
        private final Iterator<Entry<KeyHandle, Tracker>> iter = trackers.entrySet().iterator();
        private Entry<KeyHandle, Tracker> current;

        @Override public boolean advance() {
            if (!iter.hasNext()) {
                return false;
            }
            current = iter.next();
            return true;
        }

        @Override public KeyHandle asKeyHandle() {
            return toKeyHandle();
        }

        @Override public KeyHandle toKeyHandle() {
            return current.getKey();
        }

        @Override public Tracker asTracker() {
            return current.getValue();
        }

        @Override public void remove() {
            iter.remove();
        }
    }
}
