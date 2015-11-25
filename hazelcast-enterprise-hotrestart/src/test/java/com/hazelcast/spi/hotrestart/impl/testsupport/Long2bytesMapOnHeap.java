package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.spi.hotrestart.RecordDataSink;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.spi.hotrestart.impl.testsupport.MockRecordStoreBase.bytes2long;

public class Long2bytesMapOnHeap extends Long2bytesMapBase {
    private final Map<Long, byte[]> map = new HashMap<Long, byte[]>();

    @Override public void put(long key, byte[] value) {
        assert key > 0 : String.format("Attempt to put non-positive key %x", key);
        map.remove(-key);
        if (map.put(key, value) == null) {
            size++;
        }
    }

    @Override public boolean containsKey(long key) {
        assert key > 0 : String.format("Attempt to check existence of non-positive key %x", key) ;
        return map.containsKey(key);
    }

    @Override public int size() {
        return size;
    }

    @Override public L2bCursor cursor() {
        return new Cursor();
    }

    @Override public boolean copyEntry(long key, int expectedSize, RecordDataSink sink) {
        assert key > 0 : String.format("Attempt to copy entry for negative key %x", key) ;
        final byte[] value = map.get(key);
        if (value == null) {
            if (expectedSize != KEY_SIZE || !map.containsKey(-key)) {
                return false;
            }
        } else {
            if (expectedSize != KEY_SIZE + value.length) {
                return false;
            }
            sink.getValueBuffer(value.length).put(value);
        }
        sink.getKeyBuffer(KEY_SIZE).putLong(key);
        return true;
    }

    @Override public void putTombstone(long key, long tombstoneSeq) {
        if (map.remove(key) != null) {
            size--;
        }
        map.put(-key, ByteBuffer.allocate(TOMBSTONE_SEQ_SIZE).putLong(tombstoneSeq).array());
    }

    @Override public void removeTombstone(long key, long tombstoneSeq) {
        final byte[] storedTombstone = map.get(-key);
        if (storedTombstone == null) {
            return;
        }
        final long storedTombstoneSeq = bytes2long(storedTombstone);
        if (tombstoneSeq == storedTombstoneSeq) {
            map.remove(-key);
        }
    }

    @Override public void clear() {
        map.clear();
        size = 0;
    }

    @Override public int valueSize(long key) {
        if (key < 0) {
            throw new IllegalArgumentException("Negative key " + key);
        }
        final byte[] value = map.get(key);
        return value != null ? value.length : -1;
    }

    @Override public void dispose() {
    }

    private class Cursor implements L2bCursor {
        private final Iterator<Entry<Long, byte[]>> iter = map.entrySet().iterator();
        private Entry<Long, byte[]> current;

        @Override public boolean advance() {
            while (iter.hasNext()) {
                current = iter.next();
                if (current.getKey() > 0) {
                    return true;
                }
            }
            return false;
        }

        @Override public long key() {
            return current.getKey();
        }

        @Override public int valueSize() {
            return current.getValue().length;
        }
    }
}
