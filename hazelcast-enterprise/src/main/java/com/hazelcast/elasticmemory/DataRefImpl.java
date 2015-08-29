package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.DataRef;

public class DataRefImpl implements DataRef {

    private static final int INT_SIZE = 4;
    private static final int ARRAY_SIZE = 16;
    private static final int REF_SIZE = 4;
    private static final int BOOLEAN_SIZE = 1;

    private final int length;
    private final int[] chunks;

    private volatile boolean valid;

    DataRefImpl(int[] indexes, int length) {
        this.chunks = indexes;
        this.length = length;
        this.valid = true;
    }

    public boolean isEmpty() {
        return getChunkCount() == 0;
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public int heapCost() {
        // 2x integers + 1x array + 1x ref + 1x bool
        return INT_SIZE + INT_SIZE + ARRAY_SIZE + REF_SIZE + BOOLEAN_SIZE;
    }

    public int getChunkCount() {
        return chunks != null ? chunks.length : 0;
    }

    public int getChunk(int i) {
        return chunks[i];
    }

    public boolean isValid() {
        // volatile read
        return valid;
    }

    public void invalidate() {
        // volatile write
        valid = false;
    }
}
