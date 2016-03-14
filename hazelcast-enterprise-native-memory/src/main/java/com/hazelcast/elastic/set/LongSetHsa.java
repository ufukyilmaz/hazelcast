package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongCursor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray8byteKeyNoValue;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * {@link LongSet} implementation based on {@link HashSlotArray8byteKey}.
 */
public class LongSetHsa implements LongSet {

    private final long nullValue;
    private final HashSlotArray8byteKey hsa;

    public LongSetHsa(long nullValue, MemoryManager memMgr) {
        this(nullValue, memMgr, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public LongSetHsa(long nullValue, MemoryManager memMgr, int initialCapacity, float loadFactor) {
        this.nullValue = nullValue;
        this.hsa = new HashSlotArray8byteKeyNoValue(nullValue, memMgr, initialCapacity, loadFactor);
        hsa.gotoNew();
    }

    @Override
    public boolean add(long value) {
        assert value != nullValue : "add() called with null-sentinel value " + nullValue;
        return hsa.ensure(value) > 0;

    }

    @Override
    public boolean remove(long value) {
        assert value != nullValue : "remove() called with null-sentinel value " + nullValue;
        return hsa.remove(value);
    }

    @Override
    public boolean contains(long value) {
        assert value != nullValue : "contains() called with null-sentinel value " + nullValue;
        return hsa.get(value) != NULL_ADDRESS;
    }

    @Override
    public long size() {
        return hsa.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void clear() {
        hsa.clear();
    }

    @Override
    public LongCursor cursor() {
        assert hsa.address() >= 0 : "cursor() called on a disposed map";
        return new Cursor();
    }

    @Override
    public void dispose() {
        hsa.dispose();
    }

    private final class Cursor implements LongCursor {

        private final HashSlotCursor8byteKey hsaCursor = hsa.cursor();

        @Override
        public boolean advance() {
            return hsaCursor.advance();
        }

        @Override
        public long value() {
            return hsaCursor.key();
        }

        @Override
        public void reset() {
            hsaCursor.reset();
        }
    }
}
