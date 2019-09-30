package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * {@link com.hazelcast.internal.hidensity.HiDensityRecordProcessor HiDensityRecordProcessor} for Hi-Density cache implementation.
 * Only difference from a {@link DefaultHiDensityRecordProcessor} is, the method {@link #disposeDeferredBlocks}
 * doesn't touch {@link #storageInfo} during dispose.
 *
 * @param <R> the type of the {@link HiDensityRecord} to be processed.
 */
public class CacheHiDensityRecordProcessor<R extends HiDensityRecord> extends DefaultHiDensityRecordProcessor<R> {

    public CacheHiDensityRecordProcessor(EnterpriseSerializationService serializationService,
                                         HiDensityRecordAccessor<R> recordAccessor,
                                         HazelcastMemoryManager memoryManager,
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
