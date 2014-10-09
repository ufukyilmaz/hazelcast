package com.hazelcast.memory;

import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
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

    public final long malloc(long size) {
        long address = malloc.malloc(size);
        if (address <= 0L) {
            throw new OffHeapOutOfMemoryError();
        }
        return address;
    }

    public final void free(long address) {
        malloc.free(address);
    }

}
