package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * On-heap implementation of {@link SetOfKeyHandle}.
 */
public final class SetOfKeyHandleOnHeap implements SetOfKeyHandle {
    private final Set<KeyHandle> set = new HashSet<KeyHandle>();

    @Override
    public void add(KeyHandle kh) {
        set.add(kh);
    }

    @Override
    public void remove(KeyHandle kh) {
        set.remove(kh);
    }

    @Override
    public KhCursor cursor() {
        return new Cursor();
    }

    @Override
    public void dispose() { }

    private final class Cursor implements KhCursor {
        private final Iterator<KeyHandle> iter = set.iterator();
        private KeyHandle current;

        @Override
        public boolean advance() {
            if (iter.hasNext()) {
                current = iter.next();
                return true;
            }
            return false;
        }

        @Override
        public KeyHandle asKeyHandle() {
            return current;
        }
    }
}
