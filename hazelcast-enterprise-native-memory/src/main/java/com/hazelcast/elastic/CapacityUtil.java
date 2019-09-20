package com.hazelcast.elastic;

import com.hazelcast.internal.util.QuickMath;

/**
 * Utility functions related to data structure capacity calculation.
 */
public final class CapacityUtil {

    /** Maximum length of a Java array that is a power of two. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MAX_INT_CAPACITY = 1 << 30;

    /** Maximum length of an off-heap array that is a power of two. */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long MAX_LONG_CAPACITY = 1L << 62;

    /** Minimum capacity for a hash container. */
    public static final int MIN_CAPACITY = 4;

    /** Default capacity for a hash container. */
    public static final int DEFAULT_CAPACITY = 16;

    /** Default load factor. */
    public static final float DEFAULT_LOAD_FACTOR = 0.6f;

    private CapacityUtil() { }

    /** Round the capacity to the next allowed value. */
    public static long roundCapacity(long requestedCapacity) {
        if (requestedCapacity > MAX_LONG_CAPACITY) {
            throw new IllegalArgumentException(requestedCapacity + " is greater than max allowed capacity["
                    + MAX_LONG_CAPACITY + "].");
        }

        return Math.max(MIN_CAPACITY, QuickMath.nextPowerOfTwo(requestedCapacity));
    }

    /** Round the capacity to the next allowed value. */
    public static int roundCapacity(int requestedCapacity) {
        if (requestedCapacity > MAX_INT_CAPACITY) {
            throw new IllegalArgumentException(requestedCapacity + " is greater than max allowed capacity["
                + MAX_INT_CAPACITY + "].");
        }

        return Math.max(MIN_CAPACITY, QuickMath.nextPowerOfTwo(requestedCapacity));
    }

    /** Returns the next possible capacity, counting from the current buffers' size. */
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

    /** Returns the next possible capacity, counting from the current buffers' size. */
    public static long nextCapacity(long current) {
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
}
