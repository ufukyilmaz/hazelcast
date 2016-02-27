package com.hazelcast.memory;

interface GarbageCollector {

    long GC_INTERVAL = 1000;

    boolean registerGarbageCollectable(GarbageCollectable participant);

    boolean deregisterGarbageCollectable(GarbageCollectable participant);
}
