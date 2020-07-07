package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * The index key accessor which interprets the index key as a long value
 * inlined into slot in the B+tree's node.
 */
public class BPlusTreeInlinedLongAccessor implements BPlusTreeKeyAccessor {

    private final EnterpriseSerializationService ess;

    public BPlusTreeInlinedLongAccessor(EnterpriseSerializationService ess) {
        this.ess = ess;
    }

    /**
     * Returns indexKey as it is, interpreting it as a long value.
     * @param indexKey the index key
     * @param memoryAllocator the memory allocator (ignored)
     */
    @Override
    public long convertToNativeData(Comparable indexKey, MemoryAllocator memoryAllocator) {
        return (Long) indexKey;
    }

    /**
     * Returns address as it is, interpreting it as a long value.
     * @param address the index key address
     * @return
     */
    @Override
    public Comparable convertToObject(long address) {
        return address;
    }

    @Override
    public Data convertToHeapData(long address) {
        return ess.toData(address, HEAP);
    }

    @Override
    public long convertToNativeData(long address, MemoryAllocator memoryAllocator) {
        return address;
    }

    @Override
    public void disposeNativeData(long address, MemoryAllocator memoryAllocator) {
        // no-op
    }
}
