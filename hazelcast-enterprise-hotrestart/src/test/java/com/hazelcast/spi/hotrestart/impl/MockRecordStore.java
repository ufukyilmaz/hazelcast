package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.RamStore;

import java.util.Map;

interface MockRecordStore extends RamStore {
    Map<?, byte[]> ramStore();
    boolean isEmpty();
    void put(byte[] key, byte[] value);
    void remove(byte[] key);
    void clear();
}
