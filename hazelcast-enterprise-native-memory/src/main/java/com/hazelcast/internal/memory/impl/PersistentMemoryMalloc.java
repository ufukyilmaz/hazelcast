package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;

import java.io.File;

/**
 * LibMalloc implementation for non-volatile memory. Makes JNI calls to the PMDK library
 * for all allocation requests.
 * <p>
 * Creates and locks exclusively a {@link #directory} backed by a non-volatile memory
 * to store all allocated memory blocks.
 */
final class PersistentMemoryMalloc implements LibMalloc {

    private final PersistentMemoryDirectory directory;
    private final PersistentMemoryHeap pmemHeap;

    PersistentMemoryMalloc(NativeMemoryConfig config, long size) {
        assert config.getPersistentMemoryDirectory() != null;
        this.directory = new PersistentMemoryDirectory(config);
        File pmemFile = directory.getPersistentMemoryFile();
        pmemHeap = PersistentMemoryHeap.createHeap(pmemFile.getAbsolutePath(), size);
    }

    @Override
    public long malloc(long size) {
        try {
            return pmemHeap.allocate(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long realloc(long address, long size) {
        try {
            return pmemHeap.realloc(address, size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address) {
        pmemHeap.free(address);
    }

    @Override
    public void dispose() {
        pmemHeap.close();
        directory.dispose();
    }

    @Override
    public String toString() {
        return "PersistentMemoryMalloc";
    }

}
