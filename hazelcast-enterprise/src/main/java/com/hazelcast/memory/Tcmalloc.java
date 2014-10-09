package com.hazelcast.memory;

/**
 * @author mdogan 03/12/13
 */
public final class Tcmalloc extends FFIBasedMalloc {

    protected String libraryName() {
        return "tcmalloc";
    }

    @Override
    public String toString() {
        return "Tcmalloc";
    }
}
