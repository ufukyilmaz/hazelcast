package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.query.impl.Comparables;

public class DefaultBPlusTreeKeyComparator implements BPlusTreeKeyComparator {

    private final EnterpriseSerializationService ess;

    private final ThreadLocal<NativeMemoryData> rightDataHolder = new ThreadLocal<NativeMemoryData>() {
        @Override
        protected NativeMemoryData initialValue() {
            return new NativeMemoryData();
        }
    };

    public DefaultBPlusTreeKeyComparator(EnterpriseSerializationService ess) {
        this.ess = ess;
    }

    @Override
    public int compare(Comparable left, long rightAddress) {
        NativeMemoryData rightData = rightDataHolder.get();

        rightData.reset(rightAddress);

        Comparable right = null;
        if (rightData.totalSize() > 0) {
            right = ess.toObject(rightData);
        }
        if (left == null && right == null) {
            return 0;
        } else if (left != null && right != null) {
            return Comparables.compare(left, right);
        } else if (left == null) {
            return -1;
        } else {
            return 1;
        }
    }

}
