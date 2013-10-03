package com.hazelcast.elasticmemory.storage;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.SerializationConstants;

public class DataRef {

    public static final DataRef EMPTY_DATA_REF = new DataRef(SerializationConstants.CONSTANT_TYPE_DATA, null, 0, null);

    public final int length;
    public final int type;
    private final int[] chunks;
    private final ClassDefinition classDefinition;

    private volatile boolean valid;

    DataRef(int type, int[] indexes, int length, ClassDefinition cd) {
        this.type = type;
        this.chunks = indexes;
        this.length = length;
        this.classDefinition = cd;
        this.valid = true; // volatile write
    }

    public boolean isEmpty() {
        return getChunkCount() == 0;
    }

    public int getChunkCount() {
        return chunks != null ? chunks.length : 0;
    }

    public int getChunk(int i) {
        return chunks[i];
    }

    boolean isValid() {
        return valid; // volatile read
    }

    void invalidate() {
        valid = false;  // volatile write
    }

    ClassDefinition getClassDefinition() {
        return classDefinition;
    }

    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
}
