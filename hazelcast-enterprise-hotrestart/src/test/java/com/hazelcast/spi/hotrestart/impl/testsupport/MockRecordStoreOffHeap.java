package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.KeyOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

public class MockRecordStoreOffHeap extends MockRecordStoreBase {

    MockRecordStoreOffHeap(long prefix, MemoryManager memMgr, HotRestartStore hrStore) {
        super(prefix, new Long2bytesMapOffHeap(memMgr), hrStore);
    }

    @Override
    public KeyHandleOffHeap toKeyHandle(byte[] key) {
        return new SimpleHandleOffHeap(bytes2long(key), -prefix);
    }

    @Override
    HotRestartKey hrKey(long key) {
        return new KeyOffHeap(prefix, long2bytes(key), key, -prefix);
    }

    @Override
    long unwrapKey(KeyHandle kh) {
        return ((KeyHandleOffHeap) kh).address();
    }
}
