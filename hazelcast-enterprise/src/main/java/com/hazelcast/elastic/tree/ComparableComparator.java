package com.hazelcast.elastic.tree;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Comparator of the off-heap comparable values given to the compare methods as native addresses and sizes.
 * Will move the data to heap, deserialize the objects execute the comparision implemented by the Comparable contract.
 */
public class ComparableComparator implements OffHeapComparator {

    private final EnterpriseSerializationService ess;

    public ComparableComparator(EnterpriseSerializationService ess) {
        this.ess = ess;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(MemoryBlock lBlob, MemoryBlock rBlob) {
        NativeMemoryData leftData = new NativeMemoryData(lBlob.address(), lBlob.size());
        NativeMemoryData rightData = new NativeMemoryData(rBlob.address(), rBlob.size());

        if (leftData.equals(rightData)) {
            return 0;
        } else {
            Comparable left = null;
            if (leftData.totalSize() > 0) {
                left = ess.toObject(leftData);
            }
            Comparable right = null;
            if (rightData.totalSize() > 0) {
                right = ess.toObject(rightData);
            }
            if (left != null && right != null) {
                return left.compareTo(right);
            } else if (left == null) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(byte[] lBlob, byte[] rBlob) {
        Data leftData = new HeapData(lBlob);
        Data rightData = new HeapData(rBlob);

        if (leftData.equals(rightData)) {
            return 0;
        } else {
            if (leftData.totalSize() > 0 && rightData.totalSize() > 0) {
                Comparable left = ess.toObject(leftData);
                Comparable right = ess.toObject(rightData);
                return left.compareTo(right);
            } else if (leftData.totalSize() > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

}
