package com.hazelcast.memory;

/**
 * @author mdogan 17/04/14
 */
interface GarbageCollector {

    long GC_INTERVAL = 1000;

    boolean registerGarbageCollectable(GarbageCollectable participant);

    boolean deregisterGarbageCollectable(GarbageCollectable participant);

}
