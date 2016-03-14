package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray8byteKeyImpl;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.internal.memory.MemoryAllocator;

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

public class Long2bytesMapOffHeap extends Long2bytesMapBase {
    // key: long; value: pointer to value block
    private final HashSlotArray8byteKey hsa;
    private final ValueBlockAccessor vblockAccessor;
    private boolean isDisposed;

    public Long2bytesMapOffHeap(MemoryAllocator malloc) {
        this.vblockAccessor = new ValueBlockAccessor(malloc);
        this.hsa = new HashSlotArray8byteKeyImpl(Long.MIN_VALUE, new MemoryManagerBean(malloc, AMEM), 8,
                16 * 1024, DEFAULT_LOAD_FACTOR);
        hsa.gotoNew();
    }

    @Override public void put(long key, byte[] value) {
        final long vSlotAddr = vacateSlot(key);
        try {
            vblockAccessor.allocate(value);
        } catch (Error e) {
            hsa.remove(key);
            throw e;
        }
        AMEM.putLong(vSlotAddr, vblockAccessor.address());
    }

    @Override public void remove(long key) {
        final long vSlotAddr = hsa.get(key);
        if (vSlotAddr == NULL_ADDRESS) {
            return;
        }
        vblockAccessor.reset(vSlotAddr);
        vblockAccessor.delete();
        hsa.remove(key);
    }

    @Override public boolean containsKey(long key) {
        final long vSlotAddr = hsa.get(key);
        return vSlotAddr != NULL_ADDRESS;
    }

    @Override public int size() {
        return (int) hsa.size();
    }

    @Override public L2bCursor cursor() {
        return new Cursor(hsa.cursor());
    }

    @Override public boolean copyEntry(long key, int expectedSize, RecordDataSink sink) {
        final long vSlotAddr = hsa.get(key);
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
        for (HashSlotCursor8byteKey cursor = hsa.cursor(); cursor.advance();) {
            vblockAccessor.reset(cursor.valueAddress());
            vblockAccessor.delete();
        }
        hsa.clear();
    }

    @Override public int valueSize(long key) {
        final long vSlotAddr = hsa.get(key);
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
        hsa.dispose();
        isDisposed = true;
    }

    private long vacateSlot(long key) {
        long vSlotAddr = hsa.ensure(key);
        if (vSlotAddr < 0) {
            vSlotAddr = -vSlotAddr;
            vblockAccessor.reset(vSlotAddr);
            vblockAccessor.delete();
        }
        AMEM.putLong(vSlotAddr, NULL_ADDRESS);
        return vSlotAddr;
    }

    private static final class Cursor implements L2bCursor {
        private final ValueBlockAccessor vblockAccessor = new ValueBlockAccessor(null);
        private final HashSlotCursor8byteKey cursor;

        Cursor(HashSlotCursor8byteKey wrappedCursor) {
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
            return cursor.key();
        }

        @Override public int valueSize() {
            return vblockAccessor.valueSize();
        }
    }
}

