package com.hazelcast.elasticmemory;

import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.storage.DataRef;
import com.hazelcast.nio.serialization.ClassDefinition;

public class DataRefImpl implements DataRef {

    private final int length;
    private final int type;
    private final int[] chunks;
    private final ClassDefinition classDefinition;

    private volatile boolean valid;

    DataRefImpl(int type, int[] indexes, int length, ClassDefinition cd) {
        this.type = type;
        this.chunks = indexes;
        this.length = length;
        this.classDefinition = cd;
        this.valid = true; // volatile write
    }

    private DataRefImpl() {
        length = 0;
        type = SerializationConstants.CONSTANT_TYPE_DATA;
        chunks = null;
        classDefinition = null;
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

    public int getType() {
        return type;
    }

    public boolean isValid() {
        return valid; // volatile read
    }

    public void invalidate() {
        valid = false;  // volatile write
    }

    ClassDefinition getClassDefinition() {
        return classDefinition;
    }
}
