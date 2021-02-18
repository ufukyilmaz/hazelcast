package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.memory.PooledNativeMemoryStats;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.MemoryInfoAccessor;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;

import static com.hazelcast.memory.MemoryUnit.MEGABYTES;

/**
 * Checks whether a specific threshold is exceeded or not according
 * to configured {@link MaxSizePolicy} to start eviction process.
 *
 * Created one per map-container.
 */
public class HDEvictionChecker extends EvictionChecker {

    private final long maxNativeMemory;
    private final MemoryStats memoryStats;

    public HDEvictionChecker(MemoryInfoAccessor memoryInfoAccessor,
                             MapServiceContext mapServiceContext) {
        super(memoryInfoAccessor, mapServiceContext);
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        this.memoryStats = memoryManager.getMemoryStats();
        this.maxNativeMemory = memoryStats instanceof PooledNativeMemoryStats
                ? ((PooledNativeMemoryStats) memoryStats).getMaxData() : memoryStats.getMaxNative();
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        EnterpriseMapContainer mapContainer = ((EnterpriseMapContainer) recordStore.getMapContainer());
        HiDensityStorageInfo storageInfo = mapContainer.getHDStorageInfo();
        MapConfig mapConfig = mapContainer.getMapConfig();
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        MaxSizePolicy maxSizePolicy = evictionConfig.getMaxSizePolicy();
        int maxConfiguredSize = evictionConfig.getSize();

        switch (maxSizePolicy) {
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return (memoryStats.getFreeNative() * ONE_HUNDRED / maxNativeMemory) < maxConfiguredSize;
            case FREE_NATIVE_MEMORY_SIZE:
                return memoryStats.getFreeNative() < MEGABYTES.toBytes(maxConfiguredSize);
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return (storageInfo.getUsedMemory() * ONE_HUNDRED / maxNativeMemory) > maxConfiguredSize;
            case USED_NATIVE_MEMORY_SIZE:
                return storageInfo.getUsedMemory() > MEGABYTES.toBytes(maxConfiguredSize);
            default:
                return super.checkEvictable(recordStore);
        }
    }
}
