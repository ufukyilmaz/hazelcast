package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.hotrestart.RamStore;

public interface MockRecordStore extends RamStore, Disposable {

    Long2bytesMap ramStore();

    void put(long key, byte[] value);

    void remove(long key);

    void clear();
}
