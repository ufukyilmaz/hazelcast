package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.spi.hotrestart.impl.SetOfKeyHandle.KhCursor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class MockRecordStoreBase implements MockRecordStore {
    final Long2bytesMap ramStore;
    final HotRestartStore hrStore;
    final long prefix;

    MockRecordStoreBase(long prefix, Long2bytesMap ramStore, HotRestartStore hrStore) {
        this.ramStore = ramStore;
        this.prefix = prefix;
        this.hrStore = hrStore;
    }

    @Override public final Long2bytesMap ramStore() {
        return ramStore;
    }

    @Override public final void put(long key, byte[] value) {
        synchronized (ramStore) {
            ramStore.put(key, value);
        }
        hrStore.put(hrKey(key), value);
    }

    @Override public final void remove(long key) {
        synchronized (ramStore) {
            if (!ramStore.containsKey(key)) {
                return;
            }
            ramStore.remove(key);
        }
        hrStore.remove(hrKey(key));
    }

    @Override public final void clear() {
        synchronized (ramStore) {
            ramStore.clear();
        }
    }

    @Override public void removeNullEntries(SetOfKeyHandle keyHandles) {
        for (KhCursor cursor = keyHandles.cursor(); cursor.advance();) {
            ramStore.remove(unwrapKey(cursor.asKeyHandle()));
        }
    }

    @Override public final void dispose() {
        ramStore.dispose();
    }

    @Override public final boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink sink) {
        synchronized (ramStore) {
            return ramStore.copyEntry(unwrapKey(kh), expectedSize, sink);
        }
    }

    @Override public final void accept(KeyHandle kh, byte[] value) {
        assert kh != null : "accept() called with null key";
        assert value != null : "accept called with null value";
        ramStore.put(unwrapKey(kh), value);
    }

    public static byte[] long2bytes(long in) {
        return ByteBuffer.allocate(8).putLong(in).array();
    }

    public static long bytes2long(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    abstract HotRestartKey hrKey(long key);

    abstract long unwrapKey(KeyHandle kh);
}
