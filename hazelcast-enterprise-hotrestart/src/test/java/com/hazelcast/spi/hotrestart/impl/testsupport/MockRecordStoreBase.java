package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RecordDataSink;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class MockRecordStoreBase implements MockRecordStore {
    final Long2bytesMap ramStore;
    final HotRestartStore hrStore;
    final long prefix;
    private final Queue<Collection<TombstoneId>> tombstoneReleaseQueue =
            new ConcurrentLinkedQueue<Collection<TombstoneId>>();

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
            final long tombstoneSeq = hrStore.removeStep1(hrKey(key));
            ramStore.putTombstone(key, tombstoneSeq);
        }
        hrStore.removeStep2();
    }

    @Override public final void clear() {
        hrStore.clear(prefix);
        synchronized (ramStore) {
            ramStore.clear();
        }
    }

    @Override public void drainTombstoneReleaseQueue() {
        for (Collection<TombstoneId> tombstoneIds; (tombstoneIds = tombstoneReleaseQueue.poll()) != null;) {
            synchronized (ramStore) {
                for (TombstoneId tid : tombstoneIds) {
                    ramStore.removeTombstone(unwrapKey(tid.keyHandle()), tid.tombstoneSeq());
                }
            }
            Thread.yield();
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

    @Override public final void releaseTombstones(Collection<TombstoneId> tombstoneIds) {
        tombstoneReleaseQueue.add(tombstoneIds);
    }

    @Override public final void accept(KeyHandle kh, byte[] value) {
        assert kh != null : "accept() called with null key";
        assert value != null : "accept called with null value";
        ramStore.put(unwrapKey(kh), value);
    }

    @Override public final void acceptTombstone(KeyHandle kh, long seq) {
        assert kh != null : "acceptTombstone() called with null key";
        ramStore.putTombstone(unwrapKey(kh), seq);
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
