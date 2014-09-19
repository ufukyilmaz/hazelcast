package com.hazelcast.memory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

/**
 * @author mdogan 17/04/14
 */
final class SimpleGarbageCollector extends Thread implements GarbageCollector {

    private final Set<GarbageCollectable> garbageCollectables =
            newSetFromMap(new ConcurrentHashMap<GarbageCollectable, Boolean>());

    SimpleGarbageCollector() {
        this("MemoryManager-GCThread");
    }

    SimpleGarbageCollector(String name) {
        super(name);
        setDaemon(true);
    }

    @Override
    public boolean registerGarbageCollectable(GarbageCollectable participant) {
        return garbageCollectables.add(participant);
    }

    @Override
    public boolean deregisterGarbageCollectable(GarbageCollectable participant) {
        return garbageCollectables.remove(participant);
    }

    @Override
    public void run() {
        try {
            while (!isInterrupted()) {
                try {
                    Thread.sleep(GC_INTERVAL);
                } catch (InterruptedException e) {
                    return;
                }
                gc();
            }
        } finally {
            garbageCollectables.clear();
        }
    }

    private void gc() {
        for (GarbageCollectable garbageCollectable : garbageCollectables) {
            if (isInterrupted()) {
                return;
            }
            try {
                garbageCollectable.gc();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void abort() {
        if (isAlive()) {
            interrupt();
            try {
                join(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
