package com.hazelcast.map.impl.eviction;

import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Helper class to centralize Hot Restart eviction related stuff.
 */
public final class HotRestartEvictionHelper {
    /**
     * By default, hot restart requires at least 15% free HD space.
     */
    public static final int DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE = 15;
    /**
     * The name of the XML and system property for setting the hot restart required free space.
     */
    public static final String PROPERTY_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE =
            "hazelcast.hotrestart.free.native.memory.percentage";
    /**
     * XML and system property for setting the hot restart required free space
     */
    public static final HazelcastProperty HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE = new HazelcastProperty(
            PROPERTY_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE, DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);

    private final int freeNativeMemoryPercentage;

    public HotRestartEvictionHelper() {
        this.freeNativeMemoryPercentage = Integer.getInteger(
                PROPERTY_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE, DEFAULT_HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    public HotRestartEvictionHelper(HazelcastProperties properties) {
        this.freeNativeMemoryPercentage = properties.getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    /**
     * When Hot Restart is enabled, we want at least "hazelcast.hotrestart.free.native.memory.percentage" percent
     * free Hi-Density space.
     *
     * @return percentage of minimum free native memory space.
     */
    public int getHotRestartFreeNativeMemoryPercentage() {
        return freeNativeMemoryPercentage;
    }
}
