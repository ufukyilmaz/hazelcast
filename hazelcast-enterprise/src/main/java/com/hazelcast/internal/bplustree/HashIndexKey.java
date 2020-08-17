package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.serialization.DataType.HEAP;

/**
 * A wrapped of the index key to cache its on-heap serialized representation and hash value.
 * It is used for the HASH index to optimize hash value calculation.
 *
 * @param <T> the type of objects that this object may be compared to
 */
class HashIndexKey<T> implements Comparable<T> {

    private final Comparable indexKey;
    private long indexKeyHash;
    private Data indexKeyData;

    HashIndexKey(Comparable indexKey) {
        this.indexKey = indexKey;
    }

    Comparable getIndexKey() {
        return indexKey;
    }

    long getIndexKeyHash(EnterpriseSerializationService ess) {
        if (indexKeyHash == 0L) {
            indexKeyHash = getIndexKeyData(ess).hash64();
        }
        return indexKeyHash;
    }

    Data getIndexKeyData(EnterpriseSerializationService ess) {
        if (indexKeyData == null) {
            indexKeyData = ess.toData(indexKey, HEAP);
        }
        return indexKeyData;
    }

    @Override
    public int compareTo(T o) {
        return indexKey.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof HashIndexKey && indexKey.equals(((HashIndexKey) o).indexKey);
    }

    @Override
    public int hashCode() {
        return indexKey.hashCode();
    }
}
