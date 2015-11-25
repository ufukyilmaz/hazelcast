package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.util.collection.LongHashSet;

import java.util.HashSet;
import java.util.Set;

public abstract class Long2bytesMapBase implements Long2bytesMap {
    int size;

    @Override public int size() {
        return size;
    }

    @Override public Set<Long> keySet() {
        final Set<Long> keySet = new HashSet<Long>(size); // new LongHashSet(size, -1);
        for (L2bCursor cursor = cursor(); cursor.advance();) {
            keySet.add(cursor.key());
        }
        return keySet;
    }
}
