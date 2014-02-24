package com.hazelcast.examples;

import com.hazelcast.elasticmemory.util.MemoryUnit;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mdogan 27/12/13
 */
public class GCUtil {

    static final Set<String> YOUNG_GC = new HashSet<String>(3);
    static final Set<String> OLD_GC = new HashSet<String>(3);

    static {
        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");
    }


    public static String getGCStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("GC STATS :: ").append('\n');
        sb.append("=================================================================================").append('\n');

        long minorCount = 0;
        long minorTime = 0;

        long majorCount = 0;
        long majorTime = 0;

        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count >= 0) {
                if (YOUNG_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (OLD_GC.contains(gc.getName())) {
                    majorCount += count;
                    majorTime += gc.getCollectionTime();
                } else {
                    sb.append("Unknown GC: " + gc.getName() + "-> " + Arrays.toString(gc.getMemoryPoolNames()));
                }
            }
        }

        long heapUsage = MemoryUnit.BYTES.toMegaBytes(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        sb.append("MinorGC -> Count: " + minorCount + ", Time (ms): " + minorTime
                + ", MajorGC -> Count: " + majorCount + ", Time (ms): " + majorTime
                + ", Heap Usage (MB): " + heapUsage);

        return sb.toString();
    }

//    private static void printMemStats(PrintStream out) {
//        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
//        System.err.println("Heap-Usage: " + memoryMXBean.getHeapMemoryUsage());
//        System.err.println("Non-Heap-Usage: " + memoryMXBean.getNonHeapMemoryUsage());
//
//        for (MemoryPoolMXBean mem : ManagementFactory.getMemoryPoolMXBeans()) {
//            System.err.println("MemoryPool[" + mem.getName() + "]");
//            System.err.println("\ttype: " + mem.getType());
//            System.err.println("\tusage: " + mem.getUsage());
//            try {
//                System.err.println("\tusage-threshold: " + mem.getUsageThreshold());
//            } catch (UnsupportedOperationException ignored) {
//            }
//            try {
//                System.err.println("\tusage-threshold-count: " + mem.getUsageThresholdCount());
//            } catch (UnsupportedOperationException ignored) {
//            }
//            System.err.println("\tpeak-usage: " + mem.getPeakUsage());
//            System.err.println("\tcoll-usage: " + mem.getCollectionUsage());
//            try {
//                System.err.println("\tcoll-usage-threshold: " + mem.getCollectionUsageThreshold());
//            } catch (UnsupportedOperationException ignored) {
//            }
//            try {
//                System.err.println("\tcoll-usage-threshold-count: " + mem.getCollectionUsageThresholdCount());
//            } catch (UnsupportedOperationException ignored) {
//            }
//            System.err.println("\tmemory-managers: " + Arrays.toString(mem.getMemoryManagerNames()));
//        }
//    }
}
