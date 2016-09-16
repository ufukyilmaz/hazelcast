package com.hazelcast.map.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.EnumSet;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE;
import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.SYSPROP_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.String.format;
import static java.util.EnumSet.complementOf;

/**
 * Responsible for validating supported configurations of HD backed IMap and its Near Cache.
 */
public final class HDMapConfigValidator {

    private static final EnumSet<EvictionPolicy> UNSUPPORTED_HD_NEAR_CACHE_EVICTION_POLICIES
            = EnumSet.of(NONE, RANDOM);

    private static final EnumSet<MaxSizeConfig.MaxSizePolicy> UNSUPPORTED_HD_MAP_MAXSIZE_POLICIES
            = EnumSet.of(FREE_HEAP_PERCENTAGE, FREE_HEAP_SIZE, USED_HEAP_PERCENTAGE, USED_HEAP_SIZE);

    private static final ILogger LOGGER = Logger.getLogger(MapConfig.class);

    private HDMapConfigValidator() {
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig          the mapConfig
     * @param nativeMemoryConfig the nativeMemoryConfig
     */
    public static void checkHDConfig(MapConfig mapConfig, NativeMemoryConfig nativeMemoryConfig) {
        if (NATIVE != mapConfig.getInMemoryFormat()) {
            return;
        }

        checkTrue(nativeMemoryConfig.isEnabled(),
                format("Enable native memory config to use NATIVE in-memory-format for the map [%s]", mapConfig.getName()));

        logIgnoredConfig(mapConfig);

        checkMaxSizePolicy(mapConfig);

        checkHotRestartSpecificConfig(mapConfig);
    }

    private static void logIgnoredConfig(MapConfig mapConfig) {
        if (DEFAULT_MIN_EVICTION_CHECK_MILLIS != mapConfig.getMinEvictionCheckMillis()
                || DEFAULT_EVICTION_PERCENTAGE != mapConfig.getEvictionPercentage()) {

            LOGGER.warning("Beware that eviction mechanism is different for NATIVE in-memory format. "
                    + "For this in-memory format, `minEvictionCheckMillis` and `evictionPercentage` has no effect");
        }
    }

    private static void checkMaxSizePolicy(MapConfig mapConfig) {
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        if (UNSUPPORTED_HD_MAP_MAXSIZE_POLICIES.contains(maxSizePolicy)) {
            throw new IllegalArgumentException("Map maximum size policy " + maxSizePolicy
                    + " cannot be used with NATIVE in memory format."
                    + " Supported maximum size policies are: " + complementOf(UNSUPPORTED_HD_MAP_MAXSIZE_POLICIES));
        }
    }

    /**
     * When hot-restart is enabled we do want at least "hazelcast.hotrestart.free.native.memory.percentage" percent
     * free HD space.
     * <p/>
     * If configured max-size-policy is {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy#FREE_NATIVE_MEMORY_PERCENTAGE},
     * this method asserts that max-size is not below "hazelcast.hotrestart.free.native.memory.percentage"
     */
    private static void checkHotRestartSpecificConfig(MapConfig mapConfig) {
        HotRestartConfig hotRestartConfig = mapConfig.getHotRestartConfig();
        if (hotRestartConfig == null || !hotRestartConfig.isEnabled()) {
            return;
        }

        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();

        int hotRestartMinFreeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();
        int localSizeConfig = maxSizeConfig.getSize();
        if (FREE_NATIVE_MEMORY_PERCENTAGE == maxSizePolicy && localSizeConfig < hotRestartMinFreeNativeMemoryPercentage) {
            throw new IllegalArgumentException(format(
                    "There is a global limit on the minimum free native memory, settable by the system property"
                            + " %s, whose value is currently %d percent. The map %s has Hot Restart enabled, but is configured"
                            + " with %d percent, lower than the allowed minimum.",
                    SYSPROP_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE, hotRestartMinFreeNativeMemoryPercentage,
                    mapConfig.getName(), localSizeConfig)
            );
        }
    }

    /**
     * Checks preconditions to create a map proxy with Near Cache.
     *
     * @param nearCacheConfig the nearCacheConfig
     */
    public static void checkHDConfig(NearCacheConfig nearCacheConfig, NativeMemoryConfig nativeMemoryConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (NATIVE != inMemoryFormat) {
            return;
        }

        checkTrue(nativeMemoryConfig.isEnabled(), "Enable native memory config to use NATIVE in-memory-format for Near Cache");

        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();

        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        if (UNSUPPORTED_HD_NEAR_CACHE_EVICTION_POLICIES.contains(evictionPolicy)) {
            throw new IllegalArgumentException("Near-cache eviction policy " + evictionPolicy
                    + " cannot be used with NATIVE in memory format."
                    + " Supported eviction policies are : " + complementOf(UNSUPPORTED_HD_NEAR_CACHE_EVICTION_POLICIES));
        }
    }
}
