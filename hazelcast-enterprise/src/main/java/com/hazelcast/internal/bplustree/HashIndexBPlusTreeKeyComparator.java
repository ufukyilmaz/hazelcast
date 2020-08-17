package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * The HASH index comparator that compares the index keys as follows:
 *  - Calculate the index key hash (long) and compare the hash values
 *  - If the index key hash values are equal, compare the serialized index keys
 *    byte-by-byte
 */
public class HashIndexBPlusTreeKeyComparator implements BPlusTreeKeyComparator {

    private final EnterpriseSerializationService ess;

    public HashIndexBPlusTreeKeyComparator(EnterpriseSerializationService ess) {
        this.ess = ess;
    }

    @Override
    public int compare(Comparable left, long rightAddress, long rightPayload) {
        HashIndexKey leftIndexKey = (HashIndexKey) left;

        if (leftIndexKey == null) {
            return rightAddress == NULL_ADDRESS ? 0 : -1;
        }
        long leftIndexKeyHash = leftIndexKey.getIndexKeyHash(ess);
        long rightIndexKeyHash = rightPayload;

        if (leftIndexKeyHash == rightIndexKeyHash) {
            // The hashes are equal, compare serialized byte arrays
            Data leftData = leftIndexKey.getIndexKeyData(ess);
            NativeMemoryData rightData = new NativeMemoryData().reset(rightAddress);
            return compareSerializedKeys(leftData, rightData);
        } else {
            return leftIndexKeyHash < rightIndexKeyHash ? -1 : 1;
        }
    }

    @Override
    public Comparable wrapIndexKey(Comparable indexKey) {
        assert !(indexKey instanceof HashIndexKey);
        if (indexKey == null) {
            return indexKey;
        }
        HashIndexKey wrappedIndexKey = new HashIndexKey(indexKey);
        return wrappedIndexKey;
    }

    @Override
    public Comparable unwrapIndexKey(Comparable indexKey) {
        if (indexKey == null) {
            return indexKey;
        }

        if (indexKey instanceof HashIndexKey) {
            return ((HashIndexKey) indexKey).getIndexKey();
        } else {
            return indexKey;
        }
    }

}
