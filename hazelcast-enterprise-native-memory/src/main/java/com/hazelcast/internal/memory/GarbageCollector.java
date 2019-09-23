package com.hazelcast.internal.memory;

interface GarbageCollector {

    long GC_INTERVAL = 1000;

    boolean registerGarbageCollectable(GarbageCollectable participant);

    boolean deregisterGarbageCollectable(GarbageCollectable participant);
}
