package com.hazelcast.memory;

import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.util.EmptyStatement;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

public final class MemoryStatsSupport {

    public static long totalPhysicalMemory() {
        return queryPhysicalMemory("TotalPhysicalMemorySize");
    }

    public static long freePhysicalMemory() {
        return queryPhysicalMemory("FreePhysicalMemorySize");
    }

    private static long queryPhysicalMemory(String type) {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("java.lang", "type", "OperatingSystem");
            Object attribute = mBeanServer.getAttribute(name, type);
            if (attribute != null) {
                return Long.parseLong(attribute.toString());
            }
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
        return -1L;
    }

    public static MemoryUsage getHeapMemoryUsage() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    }

    static void checkFreeMemory(long size) {
        long totalMem = totalPhysicalMemory();
        if (totalMem < 0) {
            return;
        }
        long freeMem = freePhysicalMemory();
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

    public static MemoryStats getStandardMemoryStats() {
        return new StandardMemoryStats();
    }

    public static SerializableMemoryStats getSerializableMemoryStats() {
        SerializableMemoryStats stats = new SerializableMemoryStats();
        stats.setTotalPhysical(totalPhysicalMemory());
        stats.setFreePhysical(freePhysicalMemory());

        MemoryUsage memoryUsage = getHeapMemoryUsage();
        stats.setMaxHeap(memoryUsage.getMax());
        stats.setCommittedHeap(memoryUsage.getCommitted());
        stats.setUsedHeap(memoryUsage.getUsed());

        stats.setGcStats(GCStatsSupport.getSerializableGCStats());
        return stats;
    }
}
