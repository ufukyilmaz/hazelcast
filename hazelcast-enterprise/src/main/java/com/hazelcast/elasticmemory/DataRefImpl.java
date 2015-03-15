package com.hazelcast.elasticmemory;

import com.hazelcast.internal.storage.DataRef;

public class DataRefImpl implements DataRef {

    private final int length;
    private final int type;
    private final int[] chunks;

    private volatile boolean valid;

    DataRefImpl(int type, int[] indexes, int length) {
        this.type = type;
        this.chunks = indexes;
        this.length = length;
        this.valid = true; // volatile write
    }

//    private DataRefImpl() {
//        length = 0;
//        type = SerializationConstants.CONSTANT_TYPE_DATA;
//        chunks = null;
//        classDefinition = null;
//    }

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

    public int getType() {
        return type;
    }

    public boolean isValid() {
        return valid; // volatile read
    }

    public void invalidate() {
        valid = false;  // volatile write
    }
}
