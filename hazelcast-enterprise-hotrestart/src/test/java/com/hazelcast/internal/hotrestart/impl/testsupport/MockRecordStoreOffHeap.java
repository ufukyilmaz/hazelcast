package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.hotrestart.HotRestartKey;
import com.hazelcast.internal.hotrestart.HotRestartStore;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.KeyHandleOffHeap;
import com.hazelcast.internal.hotrestart.impl.KeyOffHeap;
import com.hazelcast.internal.hotrestart.impl.SimpleHandleOffHeap;

public class MockRecordStoreOffHeap extends MockRecordStoreBase {

    MockRecordStoreOffHeap(long prefix, MemoryManager memMgr, HotRestartStore hrStore, boolean fsyncEnabled) {
        super(prefix, new Long2bytesMapOffHeap(memMgr), hrStore, fsyncEnabled);
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
