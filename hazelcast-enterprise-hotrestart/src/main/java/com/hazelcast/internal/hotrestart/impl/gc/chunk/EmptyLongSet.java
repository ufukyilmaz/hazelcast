package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.util.collection.LongCursor;
import com.hazelcast.internal.util.collection.LongSet;

/**
 * Empty {@code LongSet}.
 */
final class EmptyLongSet implements LongSet, LongCursor {
    private static final EmptyLongSet INSTANCE = new EmptyLongSet();

    private EmptyLongSet() { }

    public static EmptyLongSet emptyLongSet() {
        return INSTANCE;
    }

    @Override
    public boolean add(long value) {
        throw new UnsupportedOperationException("EmptyLongSet.add");
    }

    @Override
    public boolean remove(long value) {
        return false;
    }

    @Override
    public boolean contains(long value) {
        return false;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public void clear() {
    }

    @Override
    public LongCursor cursor() {
        return this;
    }

    @Override
    public void dispose() {
    }



    // LongCursor

    @Override
    public boolean advance() {
        return false;
    }

    @Override
    public long value() {
        throw new AssertionError("Cursor is invalid");
    }

    @Override
    public void reset() {
    }
}
