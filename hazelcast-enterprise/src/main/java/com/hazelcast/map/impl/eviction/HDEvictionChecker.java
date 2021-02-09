package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryStats;
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

    private final MemoryStats memoryStats;

    public HDEvictionChecker(MemoryInfoAccessor memoryInfoAccessor,
                             MapServiceContext mapServiceContext) {
        super(memoryInfoAccessor, mapServiceContext);
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        this.memoryStats = memoryManager.getMemoryStats();
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        EnterpriseMapContainer mapContainer = ((EnterpriseMapContainer) recordStore.getMapContainer());
        HiDensityStorageInfo storageInfo = mapContainer.getHDStorageInfo();
        MapConfig mapConfig = mapContainer.getMapConfig();
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        MaxSizePolicy maxSizePolicy = evictionConfig.getMaxSizePolicy();
        int maxConfiguredSize = evictionConfig.getSize();

        boolean evictable;
        switch (maxSizePolicy) {
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                evictable = (memoryStats.getFreeNative() * ONE_HUNDRED / memoryStats.getMaxNative()) < maxConfiguredSize;
                break;
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

        return evictable;
    }
}
