package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.hotrestart.HotRestartKey;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.KeyOnHeap;

public class MockRecordStoreOnHeap extends MockRecordStoreBase {

    MockRecordStoreOnHeap(long prefix, HotRestartStore hrStore, boolean fsyncEnabled) {
        super(prefix, new Long2bytesMapOnHeap(), hrStore, fsyncEnabled);
    }

    @Override
    public KeyOnHeap toKeyHandle(byte[] key) {
        return new KeyOnHeap(prefix, key);
    }

    @Override
    HotRestartKey hrKey(long key) {
        return new KeyOnHeap(prefix, long2bytes(key));
    }

    @Override
    long unwrapKey(KeyHandle kh) {
        return bytes2long(((KeyOnHeap) kh).bytes());
    }
}
