package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.internal.serialization.Data;

import java.util.Iterator;

interface LockableNearCacheRecordStoreSegment {

    Iterator<Data> getKeySetIterator();

    void lock();

    void unlock();
}
