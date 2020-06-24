package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * The index key accessor which interprets the index key long value as a long hash value.
 */
public class BPlusTreeHashKeyAccessor implements BPlusTreeKeyAccessor {

    private final EnterpriseSerializationService ess;

    public BPlusTreeHashKeyAccessor(EnterpriseSerializationService ess) {
        this.ess = ess;
    }

    /**
     * Returns indexKey as it is, interpreting it as a hash value.
     * @param indexKey the index key
     */
    @Override
    public long convertToNativeData(Comparable indexKey) {
        return (Long) indexKey;
    }

    /**
     * Returns address as it is, interpreting it as a hash value.
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
    public long convertToNativeData(long address) {
        return address;
    }

    @Override
    public void disposeNativeData(long address) {
        // no-op
    }
}
