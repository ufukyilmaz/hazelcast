package com.hazelcast.memory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

public final class GCStatsSupport {

    private static final Set<String> YOUNG_GC = new HashSet<String>(3);
    private static final Set<String> OLD_GC = new HashSet<String>(3);

    static {
        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");
    }

    static void fill(MutableGCStats stats) {
        long minorCount = 0;
        long minorTime = 0;
        long majorCount = 0;
        long majorTime = 0;
        long unknownCount = 0;
        long unknownTime = 0;

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
                    unknownCount += count;
                    unknownTime += gc.getCollectionTime();
                }
            }
        }

        stats.setMajorCount(majorCount);
        stats.setMajorTime(majorTime);
        stats.setMinorCount(minorCount);
        stats.setMinorTime(minorTime);
        stats.setUnknownCount(unknownCount);
        stats.setUnknownTime(unknownTime);
    }

    public static GCStats getStandardGCStats() {
        return new StandardGCStats();
    }

    public static SerializableGCStats getSerializableGCStats() {
        SerializableGCStats stats = new SerializableGCStats();
        fill(stats);
        return stats;
    }
}
