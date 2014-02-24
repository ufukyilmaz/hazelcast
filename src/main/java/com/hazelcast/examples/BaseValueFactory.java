package com.hazelcast.examples;

/**
 * @author: ahmetmircik
 * Date: 2/6/14
 */
public abstract class BaseValueFactory<T> implements ValueFactory<T> {

    protected static int forcedCast(long number) {
        if (number > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        if (number <= Integer.MIN_VALUE) return 0;

        return (int) number;

    }
}
