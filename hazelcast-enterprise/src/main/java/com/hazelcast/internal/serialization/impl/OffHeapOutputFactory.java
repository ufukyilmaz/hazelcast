package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/***
 * This is factory interface to create OffHeapDataOutput instance
 * It lets us to serialize data directly to the off-heap.
 */
public interface OffHeapOutputFactory {
    /***
     * @param bufferSize - initial buffer size for serialization
     * @param service    -instance of the enterprise serialization service
     * @return OffHeapDataOutput object to read serialize data
     */
    OffHeapDataOutput createOutput(long bufferSize,
                                   EnterpriseSerializationService service);
}
