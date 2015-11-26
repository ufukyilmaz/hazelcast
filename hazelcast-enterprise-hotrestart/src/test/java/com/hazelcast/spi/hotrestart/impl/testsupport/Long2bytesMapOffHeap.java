package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.elastic.map.InlineNativeMemoryMap;
import com.hazelcast.elastic.map.InlineNativeMemoryMapImpl;
import com.hazelcast.elastic.map.InmmCursor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.RecordDataSink;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;

public class Long2bytesMapOffHeap extends Long2bytesMapBase {
    // key: long; value: pointer to value block
    private final InlineNativeMemoryMap map;
    private final ValueBlockAccessor vblockAccessor;
    private boolean isDisposed;

    public Long2bytesMapOffHeap(MemoryAllocator malloc) {
        this.vblockAccessor = new ValueBlockAccessor(malloc);
        this.map = new InlineNativeMemoryMapImpl(malloc, 8, 16*1024, 0.6f);
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
        size++;
    }

    @Override public void putTombstone(long key, long tombstoneSeq) {
        final long vSlotAddr = vacateSlot(key);
        try {
            vblockAccessor.allocateTombstone(tombstoneSeq);
        } catch (Error e) {
            map.remove(key, 0);
            throw e;
        }
        UNSAFE.putLong(vSlotAddr, vblockAccessor.address());
    }

    @Override public void removeTombstone(long key, long tombstoneSeq) {
        final long vSlotAddr = map.get(key, 0);
        if (vSlotAddr == NULL_ADDRESS) {
            return;
        }
        vblockAccessor.reset(vSlotAddr);
        if (vblockAccessor.isTombstone() && vblockAccessor.tombstoneSeq() == tombstoneSeq) {
            vblockAccessor.delete();
            map.remove(key, 0);
        }
    }

    @Override public boolean containsKey(long key) {
        final long vSlotAddr = map.get(key, 0);
        if (vSlotAddr == NULL_ADDRESS) {
            return false;
        }
        vblockAccessor.reset(vSlotAddr);
        return !vblockAccessor.isTombstone();
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
        if (vblockAccessor.isTombstone()) {
            if (expectedSize != KEY_SIZE) {
                return false;
            }
        } else {
            final int valueSize = vblockAccessor.valueSize();
            if (expectedSize != KEY_SIZE + valueSize) {
                return false;
            }
            vblockAccessor.copyInto(sink.getValueBuffer(valueSize));
        }
        sink.getKeyBuffer(KEY_SIZE).putLong(key);
        return true;
    }

    @Override public void clear() {
        for (InmmCursor cursor = map.cursor(); cursor.advance();) {
            vblockAccessor.reset(cursor.valueAddress());
            vblockAccessor.delete();
        }
        map.clear();
        size = 0;
    }

    @Override public int valueSize(long key) {
        final long vSlotAddr = map.get(key, 0);
        if (vSlotAddr == NULL_ADDRESS) {
            return -1;
        }
        vblockAccessor.reset(vSlotAddr);
        return vblockAccessor.isTombstone() ? -1 : vblockAccessor.valueSize();
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
        long vSlotAddr = map.put(key, 0);
        if (vSlotAddr < 0) {
            vSlotAddr = -vSlotAddr;
            vblockAccessor.reset(vSlotAddr);
            if (!vblockAccessor.isTombstone()) {
                size--;
            }
            vblockAccessor.delete();
            UNSAFE.putLong(vSlotAddr, NULL_ADDRESS);
        }
        return vSlotAddr;
    }

    private static final class Cursor implements L2bCursor {
        private final ValueBlockAccessor vblockAccessor = new ValueBlockAccessor(null);
        private final InmmCursor cursor;

        Cursor(InmmCursor wrappedCursor) {
            this.cursor = wrappedCursor;
        }

        @Override public boolean advance() {
            while (cursor.advance()) {
                vblockAccessor.reset(cursor.valueAddress());
                if (!vblockAccessor.isTombstone()) {
                    return true;
                }
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

