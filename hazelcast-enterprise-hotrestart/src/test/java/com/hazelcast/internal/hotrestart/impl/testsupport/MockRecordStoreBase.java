package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.hotrestart.HotRestartKey;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.RecordDataSink;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle;
import com.hazelcast.internal.hotrestart.impl.SetOfKeyHandle.KhCursor;

import java.nio.ByteBuffer;

public abstract class MockRecordStoreBase implements MockRecordStore {

    final Long2bytesMap ramStore;
    final HotRestartStore hrStore;
    final long prefix;
    private final boolean fsyncEnabled;

    MockRecordStoreBase(long prefix, Long2bytesMap ramStore, HotRestartStore hrStore, boolean fsyncEnabled) {
        this.ramStore = ramStore;
        this.prefix = prefix;
        this.hrStore = hrStore;
        this.fsyncEnabled = fsyncEnabled;
    }

    @Override
    public final Long2bytesMap ramStore() {
        return ramStore;
    }

    @Override
    public final void put(long key, byte[] value) {
        synchronized (ramStore) {
            ramStore.put(key, value);
        }
        hrStore.put(hrKey(key), value, fsyncEnabled);
    }

    @Override
    public final void remove(long key) {
        synchronized (ramStore) {
            if (!ramStore.containsKey(key)) {
                return;
            }
            ramStore.remove(key);
        }
        hrStore.remove(hrKey(key), fsyncEnabled);
    }

    @Override
    public final void clear() {
        synchronized (ramStore) {
            ramStore.clear();
        }
    }

    @Override
    public void removeNullEntries(SetOfKeyHandle keyHandles) {
        for (KhCursor cursor = keyHandles.cursor(); cursor.advance(); ) {
            final long key = unwrapKey(cursor.asKeyHandle());
            assert ramStore.valueSize(key) < 0;
            ramStore.remove(key);
        }
    }

    @Override
    public final void dispose() {
        ramStore.dispose();
    }

    @Override
    public final boolean copyEntry(KeyHandle kh, int expectedSize, RecordDataSink sink) {
        synchronized (ramStore) {
            return ramStore.copyEntry(unwrapKey(kh), expectedSize, sink);
        }
    }

    @Override
    public final void accept(KeyHandle kh, byte[] value) {
        assert kh != null : "accept() called with null key";
        assert value != null : "accept() called with null value";
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
