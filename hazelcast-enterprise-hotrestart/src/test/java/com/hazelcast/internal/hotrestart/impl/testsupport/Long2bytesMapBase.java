package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.util.collection.LongHashSet;

import java.util.Set;

public abstract class Long2bytesMapBase implements Long2bytesMap {

    @Override
    public Set<Long> keySet() {
        final Set<Long> keySet = new LongHashSet(size(), -1);
        for (L2bCursor cursor = cursor(); cursor.advance(); ) {
            keySet.add(cursor.key());
        }
        return keySet;
    }
}
