package com.hazelcast.memory;

import com.hazelcast.memory.error.OffHeapOutOfMemoryError;

/**
 * Created by emrah on 15/10/14.
 */
public class EnterpriseMemoryStatsSupport {

    static void checkFreeMemory(long size) {
        long totalMem = MemoryStatsSupport.totalPhysicalMemory();
        if (totalMem < 0) {
            return;
        }
        long freeMem = MemoryStatsSupport.freePhysicalMemory();
        if (freeMem < 0) {
            return;
        }
        if (size > freeMem) {
            throw new OffHeapOutOfMemoryError("Not enough free physical memory available!"
                    + " Cannot allocate " + MemorySize.toPrettyString(size) + "!"
                    + " Total physical memory: " + MemorySize.toPrettyString(totalMem)
                    + ", Free physical memory: " + MemorySize.toPrettyString(freeMem)
            );
        }
    }


}
