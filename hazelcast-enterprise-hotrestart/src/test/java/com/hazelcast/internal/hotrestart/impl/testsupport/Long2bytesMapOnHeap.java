package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.hotrestart.RecordDataSink;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Long2bytesMapOnHeap extends Long2bytesMapBase {

    private final Map<Long, byte[]> map = new HashMap<Long, byte[]>();

    @Override
    public void put(long key, byte[] value) {
        map.put(key, value);
    }

    @Override
    public boolean containsKey(long key) {
        return map.containsKey(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public L2bCursor cursor() {
        return new Cursor();
    }

    @Override
    public boolean copyEntry(long key, int expectedSize, RecordDataSink sink) {
        final byte[] value = map.get(key);
        sink.getKeyBuffer(KEY_SIZE).putLong(key);
        if (value == null || expectedSize != KEY_SIZE + value.length) {
            return false;
        }
        sink.getValueBuffer(value.length).put(value);
        return true;
    }

    @Override
    public void remove(long key) {
        map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int valueSize(long key) {
        final byte[] value = map.get(key);
        return value != null ? value.length : -1;
    }

    @Override
    public void dispose() {
    }

    private class Cursor implements L2bCursor {
        private final Iterator<Entry<Long, byte[]>> iter = map.entrySet().iterator();
        private Entry<Long, byte[]> current;

        @Override
        public boolean advance() {
            if (iter.hasNext()) {
                current = iter.next();
                return true;
            }
            return false;
        }

        @Override
        public long key() {
            return current.getKey();
        }

        @Override
        public int valueSize() {
            return current.getValue().length;
        }
    }
}
