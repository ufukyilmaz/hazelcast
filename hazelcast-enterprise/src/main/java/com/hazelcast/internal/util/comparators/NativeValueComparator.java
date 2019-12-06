package com.hazelcast.internal.util.comparators;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.serialization.Data;

/**
 * Comparator for the values of {@link
 * com.hazelcast.config.InMemoryFormat#NATIVE} backed data structures.
 */
public final class NativeValueComparator implements ValueComparator {

    /**
     * Native value comparator instance.
     */
    public static final ValueComparator INSTANCE = new NativeValueComparator();

    private NativeValueComparator() {
    }

    @Override
    public boolean isEqual(Object value1, Object value2, SerializationService ss) {
        if (value1 == value2) {
            return true;
        }

        if (value1 == null || value2 == null) {
            return false;
        }

        if (value1 instanceof NativeMemoryData) {
            return isNativeValueEqual((NativeMemoryData) value1, value2, ss);
        }

        if (value2 instanceof NativeMemoryData) {
            return isNativeValueEqual((NativeMemoryData) value2, value1, ss);
        }

        return ss.toData(value1).equals(ss.toData(value2));
    }

    private boolean isNativeValueEqual(NativeMemoryData nativeDataValue, Object value, SerializationService ss) {
        Data dataValue = ss.toData(value);
        return NativeMemoryDataUtil.equals(nativeDataValue.address(), dataValue);
    }
}
