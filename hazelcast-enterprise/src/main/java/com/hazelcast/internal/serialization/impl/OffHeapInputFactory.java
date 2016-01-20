package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;

/***
 * This is factory interface to create OffHeapDataInput instance
 * It lets us to de-serialize data directly from off-heap.
 */
public interface OffHeapInputFactory {
    /***
     * @param dataAddress - address of dataBlock in the off-heap memory
     * @param dataSize    - size of dataBlock in the off-heap memory
     * @param service     - instance of serialization service
     * @return OffHeapDataInput object to read de-serialized data
     */
    OffHeapDataInput createInput(long dataAddress,
                                 long dataSize,
                                 SerializationService service);
}
