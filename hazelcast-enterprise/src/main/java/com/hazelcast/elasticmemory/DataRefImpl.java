package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.DataRef;

public class DataRefImpl implements DataRef {

    private final int length;
    private final int[] chunks;

    private volatile boolean valid;

    DataRefImpl(int[] indexes, int length) {
        this.chunks = indexes;
        this.length = length;
        this.valid = true; // volatile write
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
        return 4 + 4 + 16 + 4 + 1; // 2x integers + 1x array + 1x ref + 1x bool
    }

    public int getChunkCount() {
        return chunks != null ? chunks.length : 0;
    }

    public int getChunk(int i) {
        return chunks[i];
    }

    public boolean isValid() {
        return valid; // volatile read
    }

    public void invalidate() {
        valid = false;  // volatile write
    }
}
