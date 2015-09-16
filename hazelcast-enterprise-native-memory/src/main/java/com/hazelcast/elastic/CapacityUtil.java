package com.hazelcast.elastic;

import com.hazelcast.util.QuickMath;

public class CapacityUtil {

    /**
     * Maximum capacity for an array that is of power-of-two size and still
     * allocatable in Java (not a negative int).
     */
    public static final int MAX_CAPACITY = 0x80000000 >>> 1;

    /**
     * Minimum capacity for a hash container.
     */
    public static final int MIN_CAPACITY = 4;

    /**
     * Default capacity for a hash container.
     */
    public static final int DEFAULT_CAPACITY = 16;

    /**
     * Default load factor.
     */
    public static final float DEFAULT_LOAD_FACTOR = 0.6f;

    /**
     * Round the capacity to the next allowed value.
     */
    public static int roundCapacity(int requestedCapacity) {
        if (requestedCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException(requestedCapacity + " is greater than max allowed capacity["
                + MAX_CAPACITY + "].");
        }

        return Math.max(MIN_CAPACITY, QuickMath.nextPowerOfTwo(requestedCapacity));
    }

    /**
     * Return the next possible capacity, counting from the current buffers'
     * size.
     */
    public static int nextCapacity(int current) {
        assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two.";

        if (current < MIN_CAPACITY / 2) {
            current = MIN_CAPACITY / 2;
        }

        current <<= 1;
        if (current < 0) {
            throw new RuntimeException("Maximum capacity exceeded.");
        }
        return current;
    }

    private CapacityUtil() {
    }
}
