package com.hazelcast.internal.memory.impl;

public abstract class AbstractMemkindMalloc implements LibMalloc {
    private final MemkindHeap memkindHeap;

    protected AbstractMemkindMalloc(MemkindHeap memkindHeap) {
        this.memkindHeap = memkindHeap;
    }

    @Override
    public long malloc(long size) {
        try {
            return memkindHeap.allocate(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long realloc(long address, long size) {
        try {
            return memkindHeap.realloc(address, size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address) {
        memkindHeap.free(address);
    }

    @Override
    public void dispose() {
        memkindHeap.close();
        onDispose();
    }

    protected void onDispose() {
    }
}
