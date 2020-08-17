package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil.readDataSize;

/**
 * Off-heap B+tree keys comparator
 */
public interface BPlusTreeKeyComparator {

    /**
     * Compares the provided left and right keys. Deserializes the right-hand key for comparison.
     *
     * @param left         the  left Comparable instance
     * @param rightAddress the off-heap address of the right key
     * @param rightPayload the payload of the right slot
     * @return a negative integer, zero, or a positive integer as the left-hand
     * side key is less than, equal to, or greater than the
     * right-hand side key.
     */
    int compare(Comparable left, long rightAddress, long rightPayload);


    /**
     * Compares the provided left and right keys in a serialized form. First compares the length
     * of the serialized form and if it is equal compares byte by byte.
     *
     * @param left      the  left Data instance
     * @param rightData the right Data instance
     * @return a negative integer, zero, or a positive integer as the left-hand
     * side key is less than, equal to, or greater than the
     * right-hand side key.
     */
    default int compareSerializedKeys(Data left, NativeMemoryData rightData) {
        if (left instanceof NativeMemoryData) {
            NativeMemoryData leftData = (NativeMemoryData) left;
            long leftDataAddress = leftData.address();
            long rightDataAddress = rightData.address();
            int leftDataSize = readDataSize(leftDataAddress);
            int rightDataSize = readDataSize(rightDataAddress);
            if (leftDataSize != rightDataSize) {
                return leftDataSize < rightDataSize ? -1 : 1;
            }

            for (int i = 0; i < leftDataSize; i++) {
                byte k1 = MEM.getByte(leftDataAddress + NativeMemoryData.DATA_OFFSET + i);
                byte k2 = MEM.getByte(rightDataAddress + NativeMemoryData.DATA_OFFSET + i);
                if (k1 != k2) {
                    return k1 < k2 ? -1 : 1;
                }
            }

            return 0;
        } else {
            byte[] leftArray = left.toByteArray();
            long rightDataAddress = rightData.address();
            int leftDataSize = left.dataSize();
            int rightDataSize = readDataSize(rightData.address());
            if (leftDataSize != rightDataSize) {
                return leftDataSize < rightDataSize ? -1 : 1;
            }

            int leftDataOffset = left.totalSize() - left.dataSize();

            for (int i = 0; i < leftDataSize; i++) {
                byte k1 = leftArray[leftDataOffset + i];
                byte k2 = MEM.getByte(rightDataAddress + NativeMemoryData.DATA_OFFSET + i);
                if (k1 != k2) {
                    return k1 < k2 ? -1 : 1;
                }
            }

            return 0;
        }
    }

    default Comparable wrapIndexKey(Comparable indexKey) {
        return indexKey;
    }

    default Comparable unwrapIndexKey(Comparable indexKey) {
        return indexKey;
    }

}
