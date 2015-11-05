package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RecordDataSink;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.spi.hotrestart.impl.MockRecordStoreOffHeap.isTombstone;
import static com.hazelcast.spi.hotrestart.impl.MockRecordStoreOffHeap.tombstone;
import static com.hazelcast.spi.hotrestart.impl.MockRecordStoreOffHeap.tombstoneSeq;

class MockRecordStoreOnHeap implements MockRecordStore {
    private final ConcurrentMap<KeyOnHeap, byte[]> dataStore = new ConcurrentHashMap<KeyOnHeap, byte[]>();
    private final long prefix;
    final HotRestartStore hrStore;

    MockRecordStoreOnHeap(long prefix, HotRestartStore hrStore) {
        this.prefix = prefix;
        this.hrStore = hrStore;
    }

    @Override public boolean isEmpty() {
        return dataStore.isEmpty();
    }

    @Override public Map<?, byte[]> ramStore() {
        return dataStore;
    }

    @Override public void put(byte[] key, byte[] value) {
        final KeyOnHeap hrKey = toKeyHandle(key);
        dataStore.put(hrKey, value);
        hrStore.put(hrKey, value);
    }

    @Override public void remove(byte[] key) {
        final KeyOnHeap kh = toKeyHandle(key);
        final byte[] curr = dataStore.get(kh);
        if (curr != null && !isTombstone(curr)) {
            dataStore.put(kh, tombstone(hrStore.removeStep1(kh)));
            hrStore.removeStep2();
        }
    }

    @Override public void clear() {
        hrStore.clear(prefix);
        dataStore.clear();
    }

    @Override public boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink bufs) {
        final byte[] keyBytes = ((KeyOnHeap) kh).bytes();
        final byte[] valueBytes = dataStore.get(kh);
        if (valueBytes == null) {
            return false;
        }
        if (!isTombstone(valueBytes)) {
            if (keyBytes.length + valueBytes.length != expectedSize) {
                return false;
            }
            bufs.getValueBuffer(valueBytes.length).put(valueBytes);
        } else if (keyBytes.length != expectedSize) {
            return false;
        }
        bufs.getKeyBuffer(keyBytes.length).put(keyBytes);
        return true;
    }

    @Override public void releaseTombstones(Collection<TombstoneId> keysToRelease) {
        for (TombstoneId toRelease : keysToRelease) {
            final KeyOnHeap kh = (KeyOnHeap) toRelease.keyHandle();
            final byte[] toRemove = dataStore.get(kh);
            if (toRemove != null && tombstoneSeq(toRemove) == toRelease.tombstoneSeq()) {
                dataStore.remove(kh, toRemove);
            }
        }
    }

    @Override public KeyOnHeap toKeyHandle(byte[] key) {
        return new KeyOnHeap(prefix, key);
    }

    @Override public void accept(KeyHandle hrKey, byte[] value) {
        dataStore.put((KeyOnHeap) hrKey, value);
    }

    @Override public void acceptTombstone(KeyHandle hrKey, long seq) {
        accept(hrKey, tombstone(seq));
    }
}
