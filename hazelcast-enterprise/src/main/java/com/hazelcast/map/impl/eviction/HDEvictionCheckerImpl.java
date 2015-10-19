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

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link MaxSizePolicy}
 * to start eviction process.
 *
 * @see EvictionCheckerImpl#checkEvictionPossible(RecordStore)
 */
public class HDEvictionCheckerImpl extends EvictionCheckerImpl {

    public HDEvictionCheckerImpl(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public boolean checkEvictionPossible(RecordStore recordStore) {
        EnterpriseMapContainer mapContainer = ((EnterpriseMapContainer) recordStore.getMapContainer());
        HiDensityStorageInfo storageInfo = mapContainer.getStorageInfo();
        MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        int maxSize = maxSizeConfig.getSize();
        MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();

        switch (maxSizePolicy) {
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return checkUsedNativeMemoryPercentage(maxSize, storageInfo);
            case USED_NATIVE_MEMORY_SIZE:
                return checkUsedNativeMemorySize(maxSize, storageInfo);
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return checkFreeNativeMemoryPercentage(maxSize);
            case FREE_NATIVE_MEMORY_SIZE:
                return checkFreeNativeMemorySize(maxSize);
            default:
                return super.checkEvictionPossible(recordStore);
        }
    }

    protected boolean checkUsedNativeMemorySize(double maxUsedSize, HiDensityStorageInfo storageInfo) {
        long currentUsedSize = storageInfo.getUsedMemory();
        return maxUsedSize < (1D * currentUsedSize / ONE_MEGABYTE);
    }

    protected boolean checkUsedNativeMemoryPercentage(double maxUsedPercentage, HiDensityStorageInfo storageInfo) {
        long currentUsedSize = storageInfo.getUsedMemory();

        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        long maxUsableSize = memoryManager.getMemoryStats().getMaxNativeMemory();

        return maxUsedPercentage < (1D * ONE_HUNDRED_PERCENT * currentUsedSize / maxUsableSize);
    }

    protected boolean checkFreeNativeMemoryPercentage(double maxFreePercentage) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();

        long maxUsableSize = memoryManager.getMemoryStats().getMaxNativeMemory();
        long currentFreeSize = memoryManager.getMemoryStats().getFreeNativeMemory();

        return maxFreePercentage > (1D * ONE_HUNDRED_PERCENT * currentFreeSize / maxUsableSize);
    }

    protected boolean checkFreeNativeMemorySize(double maxFreeSize) {
        SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();

        long currentFreeSize = memoryManager.getMemoryStats().getFreeNativeMemory();

        return maxFreeSize > (1D * currentFreeSize / ONE_MEGABYTE);
    }
}


