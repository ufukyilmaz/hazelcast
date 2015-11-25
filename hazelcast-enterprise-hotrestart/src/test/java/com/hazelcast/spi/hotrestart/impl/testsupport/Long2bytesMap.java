package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.util.collection.LongHashSet;

import java.util.Set;

public interface Long2bytesMap extends Disposable {

    int KEY_SIZE = 8;
    int TOMBSTONE_SEQ_SIZE = 8;

    void put(long key, byte[] value);

    /**
     * @return true if and only if the map contains a value (not tombstone) for the key.
     */
    boolean containsKey(long key);

    int size();

    L2bCursor cursor();

    boolean copyEntry(long key, int expectedSize, RecordDataSink sink);

    void putTombstone(long key, long tombstoneSeq);

    void removeTombstone(long key, long tombstoneSeq);

    void clear();

    Set<Long> keySet();

    int valueSize(long key);

    interface L2bCursor {

        boolean advance();

        long key();

        int valueSize();
    }
}
