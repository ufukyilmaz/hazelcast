package com.hazelcast.elastic.tree;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
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
    public int compare(long leftAddress, long leftSize, long rightAddress, long rightSize) {
        NativeMemoryData leftData = new NativeMemoryData().reset(leftAddress);
        NativeMemoryData rightData = new NativeMemoryData().reset(rightAddress);

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

}
