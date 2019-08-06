package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.MemoryInfoAccessor;

import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.spi.properties.GroupProperty.HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE;

/**
 * Checks whether a specific threshold is exceeded or not according
 * to configured {@link MaxSizePolicy} to start eviction process.
 *
 * Created one per map-container.
 */
public class HDEvictionChecker extends EvictionChecker {

    private final int hotRestartMinFreeNativeMemoryPercentage;
    private final MemoryStats memoryStats;

    public HDEvictionChecker(MemoryInfoAccessor memoryInfoAccessor,
                             MapServiceContext mapServiceContext) {
        super(memoryInfoAccessor, mapServiceContext);
        HazelcastProperties properties = mapServiceContext.getNodeEngine().getProperties();
        this.hotRestartMinFreeNativeMemoryPercentage = properties.getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        this.memoryStats = memoryManager.getMemoryStats();
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        EnterpriseMapContainer mapContainer = ((EnterpriseMapContainer) recordStore.getMapContainer());
        HiDensityStorageInfo storageInfo = mapContainer.getStorageInfo();
        MapConfig mapConfig = mapContainer.getMapConfig();
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        int maxConfiguredSize = maxSizeConfig.getSize();

        boolean evictable;

        switch (maxSizePolicy) {
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                // we do not need to check Hot Restart specific eviction in this case, because from the
                // configuration we know that it is at least 20 percent (so we just return the result)
                // see `HDMapConfigValidator#checkHotRestartSpecificConfig` for further information
                return isFreeNativeMemoryPercentageEvictable(maxConfiguredSize);
            case FREE_NATIVE_MEMORY_SIZE:
                evictable = memoryStats.getFreeNative() < MEGABYTES.toBytes(maxConfiguredSize);
                break;
            case USED_NATIVE_MEMORY_PERCENTAGE:
                evictable = (storageInfo.getUsedMemory() * ONE_HUNDRED / memoryStats.getMaxNative()) > maxConfiguredSize;
                break;
            case USED_NATIVE_MEMORY_SIZE:
                evictable = storageInfo.getUsedMemory() > MEGABYTES.toBytes(maxConfiguredSize);
                break;
            default:
                evictable = super.checkEvictable(recordStore);
                break;
        }

        return evictable || checkHotRestartSpecificEviction(mapConfig.getHotRestartConfig());
    }

    /**
     * When Hot Restart is enabled we want at least `hotRestartMinFreeNativeMemoryPercentage` free HD space.
     */
    private boolean checkHotRestartSpecificEviction(HotRestartConfig hotRestartConfig) {
        if (hotRestartConfig == null || !hotRestartConfig.isEnabled()) {
            return false;
        }

        return isFreeNativeMemoryPercentageEvictable(hotRestartMinFreeNativeMemoryPercentage);
    }

    private boolean isFreeNativeMemoryPercentageEvictable(int minFreePercentage) {
        return (memoryStats.getFreeNative() * ONE_HUNDRED / memoryStats.getMaxNative()) < minFreePercentage;
    }
}
