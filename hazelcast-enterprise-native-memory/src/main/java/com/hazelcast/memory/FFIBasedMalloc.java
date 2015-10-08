package com.hazelcast.memory;

import jnr.ffi.LibraryLoader;

/**
 * @author mdogan 03/12/13
 */
abstract class FFIBasedMalloc implements LibMalloc {

    private final LibMalloc malloc;

    protected FFIBasedMalloc() {
        malloc = LibraryLoader.create(LibMalloc.class).load(libraryName());
    }

    protected abstract String libraryName();

    @Override
    public final long malloc(long size) {
        return malloc.malloc(size);
    }

    @Override
    public final long realloc(long address, long size) {
        return malloc.realloc(address, size);
    }

    @Override
    public final void free(long address) {
        malloc.free(address);
    }

}
