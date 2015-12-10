package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.elastic.map.HashSlotArray;
import com.hazelcast.elastic.map.HashSlotArrayImpl;
import com.hazelcast.elastic.map.HashSlotCursor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.RecordDataSink;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;

public class Long2bytesMapOffHeap extends Long2bytesMapBase {
    // key: long; value: pointer to value block
    private final HashSlotArray map;
    private final ValueBlockAccessor vblockAccessor;
    private boolean isDisposed;

    public Long2bytesMapOffHeap(MemoryAllocator malloc) {
        this.vblockAccessor = new ValueBlockAccessor(malloc);
        this.map = new HashSlotArrayImpl(malloc, 8, 16*1024, 0.6f);
    }

    @Override public void put(long key, byte[] value) {
        final long vSlotAddr = vacateSlot(key);
        try {
            vblockAccessor.allocate(value);
        } catch (Error e) {
            map.remove(key, 0);
            throw e;
        }
        UNSAFE.putLong(vSlotAddr, vblockAccessor.address());
    }

    @Override public void remove(long key) {
        final long vSlotAddr = map.get(key, 0);
        if (vSlotAddr == NULL_ADDRESS) {
            return;
        }
        vblockAccessor.reset(vSlotAddr);
        vblockAccessor.delete();
        map.remove(key, 0);
    }

    @Override public boolean containsKey(long key) {
        final long vSlotAddr = map.get(key, 0);
        return vSlotAddr != NULL_ADDRESS;
    }

    @Override public int size() {
        return (int) map.size();
    }

    @Override public L2bCursor cursor() {
        return new Cursor(map.cursor());
    }

    @Override public boolean copyEntry(long key, int expectedSize, RecordDataSink sink) {
        final long vSlotAddr = map.get(key, 0);
        if (vSlotAddr == NULL_ADDRESS) {
            return false;
        }
        vblockAccessor.reset(vSlotAddr);
        final int valueSize = vblockAccessor.valueSize();
        if (expectedSize != KEY_SIZE + valueSize) {
            return false;
        }
        sink.getKeyBuffer(KEY_SIZE).putLong(key);
        vblockAccessor.copyInto(sink.getValueBuffer(valueSize));
        return true;
    }

    @Override public void clear() {
        for (HashSlotCursor cursor = map.cursor(); cursor.advance();) {
            vblockAccessor.reset(cursor.valueAddress());
            vblockAccessor.delete();
        }
        map.clear();
    }

    @Override public int valueSize(long key) {
        final long vSlotAddr = map.get(key, 0);
        if (vSlotAddr == NULL_ADDRESS) {
            return -1;
        }
        vblockAccessor.reset(vSlotAddr);
        return vblockAccessor.valueSize();
    }

    @Override public void dispose() {
        if (isDisposed) {
            return;
        }
        clear();
        map.dispose();
        isDisposed = true;
    }

    private long vacateSlot(long key) {
        long vSlotAddr = map.ensure(key, 0);
        if (vSlotAddr < 0) {
            vSlotAddr = -vSlotAddr;
            vblockAccessor.reset(vSlotAddr);
            vblockAccessor.delete();
            UNSAFE.putLong(vSlotAddr, NULL_ADDRESS);
        }
        return vSlotAddr;
    }

    private static final class Cursor implements L2bCursor {
        private final ValueBlockAccessor vblockAccessor = new ValueBlockAccessor(null);
        private final HashSlotCursor cursor;

        Cursor(HashSlotCursor wrappedCursor) {
            this.cursor = wrappedCursor;
        }

        @Override public boolean advance() {
            if (cursor.advance()) {
                vblockAccessor.reset(cursor.valueAddress());
                return true;
            }
            return false;
        }

        @Override public long key() {
            return cursor.key1();
        }

        @Override public int valueSize() {
            return vblockAccessor.valueSize();
        }
    }
}

