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

package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * {@link com.hazelcast.hidensity.HiDensityRecordProcessor HiDensityRecordProcessor} for hi-density cache implementation.
 * Only difference from a {@link DefaultHiDensityRecordProcessor} is, the method {@link #disposeDeferredBlocks}
 * doesn't touch {@link #storageInfo} during dispose.
 *
 * @param <R> the type of the {@link HiDensityRecord} to be processed
 */
public class CacheHiDensityRecordProcessor<R extends HiDensityRecord> extends DefaultHiDensityRecordProcessor<R> {

    public CacheHiDensityRecordProcessor(EnterpriseSerializationService serializationService,
                                         HiDensityRecordAccessor<R> recordAccessor,
                                         MemoryManager memoryManager,
                                         HiDensityStorageInfo storageInfo) {
        super(serializationService, recordAccessor, memoryManager, storageInfo);
    }

    @Override
    public void disposeDeferredBlocks() {
        MemoryBlock block;
        while ((block = deferredBlocksQueue.poll()) != null) {
            if (NULL_ADDRESS == block.address()) {
                // already disposed
                continue;
            }

            if (block instanceof NativeMemoryData) {
                recordAccessor.disposeData((NativeMemoryData) block);
            } else if (block instanceof HiDensityRecord) {
                recordAccessor.dispose((R) block);
            } else {
                memoryManager.free(block.address(), block.size());
            }
        }
    }
}
