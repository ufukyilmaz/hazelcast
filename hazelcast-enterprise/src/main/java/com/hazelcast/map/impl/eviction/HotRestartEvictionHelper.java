
package com.hazelcast.map.impl.eviction;

import static java.lang.Integer.getInteger;

/**
 * Helper class to centralize Hot Restart eviction related stuff.
 */
public final class HotRestartEvictionHelper {

    public static final int DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE = 15;
    public static final String SYSPROP_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE =
            "hazelcast.hotrestart.free.native.memory.percentage";

    private HotRestartEvictionHelper() {
    }

    /**
     * When Hot Restart is enabled, we want at least "hazelcast.hotrestart.free.native.memory.percentage" percent
     * free Hi-Density space.
     *
     * @return percentage of minimum free native memory space.
     */
    public static int getHotRestartFreeNativeMemoryPercentage() {
        return getInteger(SYSPROP_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE,
                DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
    }
}
