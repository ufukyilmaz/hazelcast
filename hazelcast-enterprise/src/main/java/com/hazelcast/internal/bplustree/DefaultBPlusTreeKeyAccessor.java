package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * The default index key accessor interpresting index key value as off-heap address.
 */
public final class DefaultBPlusTreeKeyAccessor implements BPlusTreeKeyAccessor {

    private final MemoryAllocator memoryAllocator;
    private final EnterpriseSerializationService ess;

    public DefaultBPlusTreeKeyAccessor(EnterpriseSerializationService ess, MemoryAllocator allocator) {
        this.memoryAllocator = allocator;
        this.ess = ess;
    }

    /**
     * Converts the indexKey into the off-heap Data. It is a caller's responsibility to free the allocated
     * the off-heap memory.
     *
     * @param indexKey the index key
     * @return the address of the off-heap data
     */
    @Override
    public long convertToNativeData(Comparable indexKey) {
        NativeMemoryData data = ess.toNativeData(indexKey, memoryAllocator);
        return data.address();
    }

    /**
     * Converts the off-heap data identified by the address into on-heap Comparable instance.
     *
     * @param address the index key address
     * @return the Comparable instance
     */
    @Override
    public Comparable convertToObject(long address) {
        NativeMemoryData indexKeyData = new NativeMemoryData().reset(address);
        return ess.toObject(indexKeyData);
    }

    @Override
    public Data convertToHeapData(long address) {
        NativeMemoryData indexKeyData = new NativeMemoryData().reset(address);
        return ess.toData(indexKeyData, HEAP);
    }

    /**
     * Converts the off-heap data identified by the address into either on-heap data or a new copy
     * of the off-heap data.
     *
     * @param address the index key address
     * @return the new data address
     */
    @Override
    public long convertToNativeData(long address) {
        NativeMemoryData indexKeyData = new NativeMemoryData().reset(address);
        return cloneNativeMemory(indexKeyData).address();
    }

    @Override
    public void disposeNativeData(long address) {
        NativeMemoryData data = new NativeMemoryData().reset(address);
        ess.disposeData(data, memoryAllocator);
    }

    /**
     * Creates a new copy of the native data
     *
     * @param data the source data
     * @return a cloned native memory
     */
    private NativeMemoryData cloneNativeMemory(NativeMemoryData data) {
        long address = memoryAllocator.allocate(data.size());
        data.copyTo(0, null, address, data.size());
        return new NativeMemoryData(address, data.size());
    }
}
