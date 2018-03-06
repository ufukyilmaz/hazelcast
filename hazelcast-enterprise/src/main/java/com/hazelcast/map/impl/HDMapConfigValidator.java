package com.hazelcast.map.impl;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.eviction.HotRestartEvictionHelper;

import java.util.EnumSet;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE;
import static com.hazelcast.internal.config.ConfigValidator.checkMergePolicy;
import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.String.format;
import static java.util.EnumSet.complementOf;

/**
 * Responsible for validating supported configurations of Hi-Density backed IMap and its Near Cache.
 */
public final class HDMapConfigValidator {

    private static final EnumSet<MaxSizeConfig.MaxSizePolicy> UNSUPPORTED_HD_MAP_MAXSIZE_POLICIES
            = EnumSet.of(FREE_HEAP_PERCENTAGE, FREE_HEAP_SIZE, USED_HEAP_PERCENTAGE, USED_HEAP_SIZE);

    private static final ILogger LOGGER = Logger.getLogger(MapConfig.class);
    private final int hotRestartMinFreeNativeMemoryPercentage;

    public HDMapConfigValidator(HotRestartEvictionHelper hotRestartEvictionHelper) {
        this.hotRestartMinFreeNativeMemoryPercentage = hotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage();
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig          the mapConfig
     * @param nativeMemoryConfig the nativeMemoryConfig
     */
    public void checkHDConfig(MapConfig mapConfig, NativeMemoryConfig nativeMemoryConfig) {
        if (NATIVE != mapConfig.getInMemoryFormat()) {
            return;
        }

        checkTrue(nativeMemoryConfig.isEnabled(),
                format("Enable native memory config to use NATIVE in-memory-format for the map [%s]", mapConfig.getName()));


        logIgnoredConfig(mapConfig);

        checkMaxSizePolicy(mapConfig);

        checkMergePolicy(mapConfig.isStatisticsEnabled(), mapConfig.getMergePolicyConfig().getPolicy());

        checkHotRestartSpecificConfig(mapConfig);
    }

    @SuppressWarnings("deprecation")
    private void logIgnoredConfig(MapConfig mapConfig) {
        if (DEFAULT_MIN_EVICTION_CHECK_MILLIS != mapConfig.getMinEvictionCheckMillis()
                || DEFAULT_EVICTION_PERCENTAGE != mapConfig.getEvictionPercentage()) {

            LOGGER.warning("`minEvictionCheckMillis` and `evictionPercentage` options"
                    + " are deprecated due to a change of the eviction mechanism."
                    + " Eviction mechanism uses a probabilistic algorithm based on sampling."
                    + " Please see documentation for further details.");
        }
    }

    private void checkMaxSizePolicy(MapConfig mapConfig) {
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        if (UNSUPPORTED_HD_MAP_MAXSIZE_POLICIES.contains(maxSizePolicy)) {
            throw new IllegalArgumentException("Map maximum size policy " + maxSizePolicy
                    + " cannot be used with NATIVE in memory format."
                    + " Supported maximum size policies are: " + complementOf(UNSUPPORTED_HD_MAP_MAXSIZE_POLICIES));
        }
    }

    /**
     * When Hot Restart is enabled, we want at least "hazelcast.hotrestart.free.native.memory.percentage" percent
     * free Hi-Density memory space.
     *
     * If configured max-size-policy is {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy#FREE_NATIVE_MEMORY_PERCENTAGE},
     * this method asserts that max-size is not below "hazelcast.hotrestart.free.native.memory.percentage".
     */
    private void checkHotRestartSpecificConfig(MapConfig mapConfig) {
        HotRestartConfig hotRestartConfig = mapConfig.getHotRestartConfig();
        if (hotRestartConfig == null || !hotRestartConfig.isEnabled()) {
            return;
        }

        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();

        int localSizeConfig = maxSizeConfig.getSize();
        if (FREE_NATIVE_MEMORY_PERCENTAGE == maxSizePolicy && localSizeConfig < hotRestartMinFreeNativeMemoryPercentage) {
            throw new IllegalArgumentException(format(
                    "There is a global limit on the minimum free native memory, settable by the system property"
                            + " %s, whose value is currently %d percent. The map %s has Hot Restart enabled, but is configured"
                            + " with %d percent, lower than the allowed minimum.",
                    HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE, hotRestartMinFreeNativeMemoryPercentage,
                    mapConfig.getName(), localSizeConfig)
            );
        }
    }
}
