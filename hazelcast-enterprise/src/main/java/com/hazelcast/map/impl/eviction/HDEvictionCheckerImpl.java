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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link MaxSizePolicy}
 * to start eviction process.
 * <p/>
 * Created one per map-container.
 *
 * @see EvictionCheckerImpl#checkEvictionPossible(RecordStore)
 */
public class HDEvictionCheckerImpl extends EvictionCheckerImpl {

    private final int hotRestartMinFreeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();

    public HDEvictionCheckerImpl(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public boolean checkEvictionPossible(RecordStore recordStore) {
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
                evictable = super.checkEvictionPossible(recordStore);
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

    protected boolean checkMaxUsedNativeMemorySize(double maxUsedSize, HiDensityStorageInfo storageInfo) {
        long currentUsedSize = storageInfo.getUsedMemory();
        return maxUsedSize < (1D * currentUsedSize / ONE_MEGABYTE);
    }

    protected boolean checkMaxUsedNativeMemoryPercentage(double maxUsedPercentage, HiDensityStorageInfo storageInfo) {
        long currentUsedSize = storageInfo.getUsedMemory();

        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        long maxUsableSize = memoryManager.getMemoryStats().getMaxNativeMemory();

        return maxUsedPercentage < (1D * ONE_HUNDRED_PERCENT * currentUsedSize / maxUsableSize);
    }

    protected boolean checkMinFreeNativeMemoryPercentage(double minFreePercentage) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();

        long maxUsableSize = memoryManager.getMemoryStats().getMaxNativeMemory();
        long currentFreeSize = memoryManager.getMemoryStats().getFreeNativeMemory();

        return minFreePercentage > (1D * ONE_HUNDRED_PERCENT * currentFreeSize / maxUsableSize);
    }

    protected boolean checkMinFreeNativeMemorySize(double minFreeSize) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();

        long currentFreeSize = memoryManager.getMemoryStats().getFreeNativeMemory();

        return minFreeSize > (1D * currentFreeSize / ONE_MEGABYTE);
    }
}


