package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;

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
