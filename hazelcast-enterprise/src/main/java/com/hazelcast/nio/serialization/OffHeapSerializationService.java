package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.impl.OffHeapDataInput;
import com.hazelcast.internal.serialization.impl.OffHeapDataOutput;

/**
 * Serialization service which return OffHeapDataInput instance.
 * It lets us to de-serialize data directly from off-heap.
 */
public interface OffHeapSerializationService {

    /**
     * @param dataAddress - address of dataBlock in the off-heap memory
     * @param dataSize    - size of dataBlock in the off-heap memory
     * @return OffHeapDataInput object to read de-serialized data
     */
    OffHeapDataInput createOffHeapObjectDataInput(long dataAddress, long dataSize);

    OffHeapDataOutput createOffHeapObjectDataOutput(long bufferSize);
}
