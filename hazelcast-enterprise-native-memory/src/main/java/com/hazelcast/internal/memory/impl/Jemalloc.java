package com.hazelcast.internal.memory.impl;

/**
 * @author mdogan 03/12/13
 */
public final class Jemalloc extends FFIBasedMalloc {

    protected String libraryName() {
        return "jemalloc";
    }

    @Override
    public String toString() {
        return "Jemalloc";
    }
}
