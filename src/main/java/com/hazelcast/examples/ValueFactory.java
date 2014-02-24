package com.hazelcast.examples;

/**
* @author mdogan 04/02/14
*/
public interface ValueFactory<T> {

    T newValue(long seed);
}
