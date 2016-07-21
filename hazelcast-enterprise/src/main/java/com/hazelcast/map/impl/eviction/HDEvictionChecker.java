/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.MemoryInfoAccessor;

import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link MaxSizePolicy}
 * to start eviction process.
 * <p/>
 * Created one per map-container.
 */
public class HDEvictionChecker extends EvictionChecker {

    private final int hotRestartMinFreeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();

    public HDEvictionChecker(MemoryInfoAccessor memoryInfoAccessor, MapServiceContext mapServiceContext) {
        super(memoryInfoAccessor, mapServiceContext);
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        EnterpriseMapContainer mapContainer = ((EnterpriseMapContainer) recordStore.getMapContainer());
        HiDensityStorageInfo storageInfo = mapContainer.getStorageInfo();
        MapConfig mapConfig = mapContainer.getMapConfig();
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        int maxSize = maxSizeConfig.getSize();
        MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();

        boolean evictable;

        switch (maxSizePolicy) {
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                // here we do not need to check hot-restart specific eviction.
                // because from configuration we know that it is at least 20 percent.
                // So just return the result. For further info. please see `HDMapConfigValidator#checkHotRestartSpecificConfig`
                return checkMinFreeNativeMemoryPercentage(maxSize);
            case FREE_NATIVE_MEMORY_SIZE:
                evictable = checkMinFreeNativeMemorySize(maxSize);
                break;
            case USED_NATIVE_MEMORY_PERCENTAGE:
                evictable = checkMaxUsedNativeMemoryPercentage(maxSize, storageInfo);
                break;
            case USED_NATIVE_MEMORY_SIZE:
                evictable = checkMaxUsedNativeMemorySize(maxSize, storageInfo);
                break;
            default:
                evictable = super.checkEvictable(recordStore);
                break;
        }

        return evictable || checkHotRestartSpecificEviction(mapConfig);

    }

    /**
     * When hot-restart is enabled we want at least `hotRestartMinFreeNativeMemoryPercentage` free HD space.
     */
    protected boolean checkHotRestartSpecificEviction(MapConfig mapConfig) {
        HotRestartConfig hotRestartConfig = mapConfig.getHotRestartConfig();
        if (hotRestartConfig == null || !hotRestartConfig.isEnabled()) {
            return false;
        }

        return checkMinFreeNativeMemoryPercentage(hotRestartMinFreeNativeMemoryPercentage);
    }

    protected boolean checkMaxUsedNativeMemorySize(int maxUsedMB, HiDensityStorageInfo storageInfo) {
        long currentUsedBytes = storageInfo.getUsedMemory();
        return MEGABYTES.toBytes(maxUsedMB) < currentUsedBytes;
    }

    protected boolean checkMaxUsedNativeMemoryPercentage(int maxUsedPercentage, HiDensityStorageInfo storageInfo) {
        long currentUsedSize = storageInfo.getUsedMemory();

        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        long maxUsableSize = memoryManager.getMemoryStats().getMaxNative();

        return maxUsedPercentage < (1D * ONE_HUNDRED_PERCENT * currentUsedSize / maxUsableSize);
    }

    protected boolean checkMinFreeNativeMemoryPercentage(int minFreePercentage) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();

        long maxUsableSize = memoryManager.getMemoryStats().getMaxNative();
        long currentFreeSize = memoryManager.getMemoryStats().getFreeNative();

        return minFreePercentage > (1D * ONE_HUNDRED_PERCENT * currentFreeSize / maxUsableSize);
    }

    protected boolean checkMinFreeNativeMemorySize(int minFreeMB) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();

        long currentFreeBytes = memoryManager.getMemoryStats().getFreeNative();

        return MEGABYTES.toBytes(minFreeMB) > currentFreeBytes;
    }
}


