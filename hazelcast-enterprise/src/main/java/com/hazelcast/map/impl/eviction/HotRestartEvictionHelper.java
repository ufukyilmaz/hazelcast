
package com.hazelcast.map.impl.eviction;

import static java.lang.Integer.getInteger;

/**
 * Helper class to centralize hot-restart eviction related stuff.
 */
public final class HotRestartEvictionHelper {

    private static final int DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE = 15;

    private HotRestartEvictionHelper() {
    }

    /**
     * When hot-restart is enabled we do want at least "hazelcast.hotrestart.free.native.memory.percentage" percent
     * free HD space.
     *
     * @return percentage of minimum free native memory space.
     */
    public static int getHotRestartFreeNativeMemoryPercentage() {
        return getInteger("hazelcast.hotrestart.free.native.memory.percentage",
                DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
    }
}
