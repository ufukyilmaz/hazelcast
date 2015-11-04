package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.RecordDataSink;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class MockRecordStoreOffHeap implements MockRecordStore {
    private final Map<ArrayKey, byte[]> dataStore = new HashMap<ArrayKey, byte[]>();
    private final Map<SimpleHandleOffHeap, ArrayKey> handle2key = new HashMap<SimpleHandleOffHeap, ArrayKey>();
    private final Map<ArrayKey, SimpleHandleOffHeap> key2handle = new HashMap<ArrayKey, SimpleHandleOffHeap>();
    private final long prefix;
    private final HotRestartStore hrStore;
    private long addrSeq;
    private long seqIdSeq;

    MockRecordStoreOffHeap(long prefix, HotRestartStore hrStore) {
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
        final HotRestartKeyOffHeap hrKey;
        synchronized (this) {
            final SimpleHandleOffHeap handle = key2handle.get(new ArrayKey(key));
            if (handle == null) {
                hrKey = hrKey(key);
                accept(hrKey.handle(), value);
            } else {
                final ArrayKey arrayKey = handle2key.get(handle);
                hrKey = new HotRestartKeyOffHeap(prefix, arrayKey, handle);
                dataStore.put(arrayKey, value);
            }
        }
        hrStore.put(hrKey, value);
    }

    @Override public void remove(byte[] key) {
        final ArrayKey arrayKey = new ArrayKey(key);
        final long tombstoneSeq;
        synchronized (this) {
            final byte[] curr = dataStore.get(arrayKey);
            if (curr != null && !isTombstone(curr)) {
                tombstoneSeq = hrStore.removeStep1(new HotRestartKeyOffHeap(prefix, arrayKey, key2handle.get(arrayKey)));
                dataStore.put(arrayKey, tombstone(tombstoneSeq));
            } else {
                tombstoneSeq = 0;
            }
        }
        if (tombstoneSeq != 0) {
            hrStore.removeStep2();
        }
    }

    @Override public void clear() {
        hrStore.clear(prefix);
        synchronized (this) {
            dataStore.clear();
            key2handle.clear();
            handle2key.clear();
        }
    }

    @Override public synchronized boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink bufs) {
        final SimpleHandleOffHeap ohk = (SimpleHandleOffHeap) kh;
        final ArrayKey arrayKey = handle2key.get(ohk);
        if (arrayKey == null) {
            return false;
        }
        final byte[] keyBytes = arrayKey.bytes;
        final byte[] valueBytes = dataStore.get(arrayKey);
        if (valueBytes == null) {
            return false;
        }
        if (!isTombstone(valueBytes)) {
            if (keyBytes.length + valueBytes.length != expectedSize) {
                return false;
            }
            bufs.getValueBuffer(valueBytes.length).put(valueBytes).flip();
        } else if (keyBytes.length != expectedSize) {
            return false;
        }
        bufs.getKeyBuffer(keyBytes.length).put(keyBytes).flip();
        return true;
    }

    @Override public synchronized void releaseTombstones(Collection<TombstoneId> tombstoneIds) {
        for (TombstoneId id : tombstoneIds) {
            final SimpleHandleOffHeap handle = (SimpleHandleOffHeap) id.keyHandle();
            final ArrayKey key = handle2key.get(handle);
            final byte[] removed = dataStore.remove(key);
            if (removed != null && tombstoneSeq(removed) != id.tombstoneSeq()) {
                dataStore.put(key, removed);
            } else {
                handle2key.remove(handle);
                key2handle.remove(key);
            }
        }
    }

    @Override public KeyHandleOffHeap toKeyHandle(byte[] key) {
        return hrKey(key).handle();
    }

    @Override public void accept(KeyHandle kh, byte[] value) {
        dataStore.put(handle2key.get(kh), value);
    }

    @Override public void acceptTombstone(KeyHandle kh, long seq) {
        accept(kh, tombstone(seq));
    }

    private HotRestartKeyOffHeap hrKey(byte[] key) {
        ArrayKey aKey = new ArrayKey(key);
        SimpleHandleOffHeap handle = key2handle.get(aKey);
        if (handle == null) {
            handle = newHandle();
            key2handle.put(aKey, handle);
            handle2key.put(handle, aKey);
        } else {
            aKey = handle2key.get(handle);
        }
        return new HotRestartKeyOffHeap(prefix, aKey, handle);
    }

    private SimpleHandleOffHeap newHandle() {
        return new SimpleHandleOffHeap(32 * ++addrSeq, prefix);
    }

    private static final byte[] TOMBSTONE_PREFIX = {'t','o','m','b'};

    static boolean isTombstone(byte[] value) {
        if (value.length != TOMBSTONE_PREFIX.length + 8) {
            return false;
        }
        for (int i = 0; i < TOMBSTONE_PREFIX.length; i++) {
            if (value[i] != TOMBSTONE_PREFIX[i]) {
                return false;
            }
        }
        return true;
    }

    static long tombstoneSeq(byte[] value) {
        if (!isTombstone(value)) {
            return 0;
        }
        long prefix = 0;
        for (int i = TOMBSTONE_PREFIX.length; i < value.length; i++) {
            prefix = (prefix << 8) | (value[i] & 0xFFL);
        }
        return prefix;
    }

    static byte[] tombstone(long seq) {
        final byte[] tombstone = Arrays.copyOf(TOMBSTONE_PREFIX, TOMBSTONE_PREFIX.length + 8);
        for (int i = tombstone.length - 1; i >= TOMBSTONE_PREFIX.length; i--) {
            tombstone[i] = (byte) seq;
            seq >>>= 8;
        }
        return tombstone;
    }
}


class ArrayKey {
    final byte[] bytes;
    final int hashCode;

    ArrayKey(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = ByteBuffer.wrap(bytes).getInt();
    }

    @Override public boolean equals(Object obj) {
        return this == obj || obj instanceof ArrayKey && Arrays.equals(this.bytes, ((ArrayKey) obj).bytes);
    }

    @Override public int hashCode() {
        return hashCode;
    }

    @Override public String toString() {
        return String.valueOf(hashCode);
    }
}

class HotRestartKeyOffHeap implements HotRestartKey {
    final long prefix;
    final ArrayKey arrayKey;
    private final SimpleHandleOffHeap addressKey;

    HotRestartKeyOffHeap(long prefix, ArrayKey arrayKey, SimpleHandleOffHeap addressKey) {
        this.prefix = prefix;
        this.arrayKey = arrayKey;
        this.addressKey = addressKey;
    }

    @Override public long prefix() {
        return prefix;
    }

    @Override public byte[] bytes() {
        return arrayKey.bytes;
    }

    @Override public SimpleHandleOffHeap handle() {
        return addressKey;
    }
}
