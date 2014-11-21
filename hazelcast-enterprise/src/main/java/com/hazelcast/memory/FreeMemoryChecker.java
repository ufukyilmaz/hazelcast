package com.hazelcast.memory;

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

/**
 * @author mdogan 21/11/14
 */
public final class FreeMemoryChecker {

    private static final boolean ENABLED;

    static {
        String checkFreeMemoryProp = System.getProperty("hazelcast.hidensity.check.freememory", "true");
        ENABLED = Boolean.parseBoolean(checkFreeMemoryProp);
    }

    private FreeMemoryChecker() {
    }

    public static void checkFreeMemory(long size) {
        if (!ENABLED) {
            return;
        }

        long totalMem = totalPhysicalMemory();
        if (totalMem < 0) {
            return;
        }
        long freeMem = freePhysicalMemory();
        if (freeMem < 0) {
            return;
        }
        if (size > freeMem) {
            throw new NativeOutOfMemoryError("Not enough free physical memory available!"
                    + " Cannot allocate " + MemorySize.toPrettyString(size) + "!"
                    + " Total physical memory: " + MemorySize.toPrettyString(totalMem)
                    + ", Free physical memory: " + MemorySize.toPrettyString(freeMem)
            );
        }
    }
}
