package com.hazelcast.memory;

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

final class FreeMemoryChecker {

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
            throw new NativeOutOfMemoryError(String.format("Not enough free physical memory available! "
                    + "Cannot allocate " + MemorySize.toPrettyString(size) + "!"
                    + " Total physical memory: " + MemorySize.toPrettyString(totalMem)
                    + " Free physical memory: " + MemorySize.toPrettyString(freeMem) + "%n"
                    + "Depending on the operating system or virtualization technology the memory check may report incorrect "
                    + "total or free amount of memory.%n"
                    + "You can disable the check by adding the following runtime switch "
                    + "'-Dhazelcast.hidensity.check.freememory=false'"));
        }
    }
}
