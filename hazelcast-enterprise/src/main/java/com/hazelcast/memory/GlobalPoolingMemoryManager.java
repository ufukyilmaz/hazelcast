package com.hazelcast.memory;

import com.hazelcast.elastic.queue.LongLinkedBlockingQueue;
import com.hazelcast.elastic.queue.LongQueue;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mdogan 03/12/13
 */
final class GlobalPoolingMemoryManager
        extends AbstractPoolingMemoryManager
        implements MemoryManager {

    private static final int HEADER_OFFSET = 4;
    // using sign bit as available bit, since offset is already positive
    private static final int AVAILABLE_BIT = Integer.SIZE - 1;
    private static final int INITIAL_CAPACITY = 2048;

    private final GarbageCollector gc;
    private final Set<Long> allocations = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    private volatile long lastFullCompaction;

    GlobalPoolingMemoryManager(int minBlockSize, int pageSize,
            LibMalloc malloc, PooledNativeMemoryStats stats, GarbageCollector gc) {
        super(minBlockSize, pageSize, malloc, stats);
        this.gc = gc;
        initializeAddressQueues();
    }

    @Override
    protected AddressQueue createAddressQueue(int index, int memorySize) {
        return new GlobalAddressQueue(index, memorySize);
    }

    @Override
    protected int getHeaderSize() {
        return HEADER_OFFSET;
    }

    @Override
    protected void onMalloc(long address) {
        assertNotNullPtr(address);
        boolean added = allocations.add(address);
        assert added : "Duplicate malloc() for address: " + address;
        lastFullCompaction = 0L;
    }

    @Override
    protected void onFree(long address) {
        assertNotNullPtr(address);
        boolean removed = allocations.remove(address);
        assert removed : "Unknown address is freed: " + address;
        lastFullCompaction = 0L;
    }

    @Override
    protected void onOome(NativeOutOfMemoryError e) {
        long now = Clock.currentTimeMillis();
        if (now > lastFullCompaction + GarbageCollector.GC_INTERVAL) {
            // immediately set compaction time to avoid multiple (as many as possible) threads to run compact()
            lastFullCompaction = now;
            compact();
            // set real compaction time
            lastFullCompaction = now;
        }
    }

    @Override
    protected void initialize(long address, int size, int offset) {
        assertNotNullPtr(address);
        assert !Bits.isBitSet(size, AVAILABLE_BIT) : "Invalid size: negative! " + size;
        assert offset >= 0 : "Invalid offset: negative! " + offset;

        int header = Bits.setBit(size, AVAILABLE_BIT);
        long value = Bits.combineToLong(offset, header);

        if (!UnsafeHelper.UNSAFE.compareAndSwapLong(null, address, 0L, value)) {
            throw new IllegalArgumentException("Wrong size, cannot initialize! Address: " + address
                    + ", Size: " + size + ", Header: " + getSizeInternal(address));
        }
    }

    @Override
    protected void markAvailable(long address) {
        assertNotNullPtr(address);

        long value = UnsafeHelper.UNSAFE.getLongVolatile(null, address);
        int size = Bits.extractInt(value, true);
        assert !Bits.isBitSet(size, AVAILABLE_BIT) : "Address already marked as available! " + address;

        int header = Bits.setBit(size, AVAILABLE_BIT);

        long base = getPage(address, size);
        if (base < 0) {
            throw new IllegalArgumentException("Address: " + address + " does not belong to this memory pool!");
        }
        int offset = (int) (address - base);
        assert offset >= 0 : "Invalid offset: " + offset;

        value = Bits.combineToLong(offset, header);
        UnsafeHelper.UNSAFE.putLongVolatile(null, address, value);
    }

    @Override
    protected boolean markUnavailable(long address, int expectedSize) {
        assertNotNullPtr(address);

        long value = UnsafeHelper.UNSAFE.getLongVolatile(null, address);
        int header = Bits.extractInt(value, true);
        // This memory address may be merged up (after acquired but not marked as unavailable yet)
        // as buddy by our GarbageCollector thread so its size may be changed.
        // In this case, it must be discarded since it is not served by its current address queue.
        if (Bits.clearBit(header, AVAILABLE_BIT) != expectedSize) {
            return false;
        }
        int offset = Bits.extractInt(value, false);

        long expected = Bits.combineToLong(offset, Bits.setBit(header, AVAILABLE_BIT));
        long update = Bits.combineToLong(0, Bits.clearBit(header, AVAILABLE_BIT));

        return UnsafeHelper.UNSAFE.compareAndSwapLong(null, address, expected, update);
    }

    @Override
    protected boolean isAvailable(long address) {
        assertNotNullPtr(address);
        int b = UnsafeHelper.UNSAFE.getIntVolatile(null, address);
        return Bits.isBitSet(b, AVAILABLE_BIT);
    }

    @Override
    protected boolean markInvalid(long address, int expectedSize) {
        assertNotNullPtr(address);

        int offset = UnsafeHelper.UNSAFE.getIntVolatile(null, address + HEADER_OFFSET);
        int header = Bits.setBit(expectedSize, AVAILABLE_BIT);

        long expected = Bits.combineToLong(offset, header);
        return UnsafeHelper.UNSAFE.compareAndSwapLong(null, address, expected, 0);
    }

    @Override
    protected boolean isValidAndAvailable(long address, int expectedSize) {
        assertNotNullPtr(address);

        long value = UnsafeHelper.UNSAFE.getLongVolatile(null, address);
        int size = Bits.extractInt(value, true);

        boolean available = Bits.isBitSet(size, AVAILABLE_BIT);
        if (!available) {
            return false;
        }

        size = Bits.clearBit(size, AVAILABLE_BIT);
        if (size != expectedSize) {
            return false;
        }

        int offset = Bits.extractInt(value, false);
        if (offset < 0 || QuickMath.modPowerOfTwo(offset, size) != 0) {
            return false;
        }
        return allocations.contains(address - offset);
    }

    protected int getSizeInternal(long address) {
        int size = UnsafeHelper.UNSAFE.getIntVolatile(null, address);
        size = Bits.clearBit(size, AVAILABLE_BIT);
        return size;
    }

    @Override
    public int getSize(long address) {
        return getSizeInternal(address - getHeaderSize());
    }

    @Override
    protected int getOffset(long address) {
        return UnsafeHelper.UNSAFE.getIntVolatile(null, address + HEADER_OFFSET);
    }

    @Override
    public int getHeaderLength() {
        return HEADER_OFFSET;
    }

    @Override
    public long getPage(long address) {
        int size = getSizeInternal(address);
        return getPage(address, size);
    }

    private long getPage(long address, int size) {
        Iterator<Long> iterator = allocations.iterator();
        long page = -1L;
        while (iterator.hasNext()) {
            long a = iterator.next();
            if (a <= address && (a + pageSize) >= (address + size)) {
                page = a;
                break;
            }
        }
        return page;
    }

    public final void destroy() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < addressQueues.length; i++) {
            AddressQueue q = addressQueues[i];
            if (q != null) {
                q.destroy();
                addressQueues[i] = null;
            }
        }
        if (!allocations.isEmpty()) {
            for (Long address : allocations) {
                pageAllocator.free(address, pageSize);
            }
            allocations.clear();
        }
    }

    @Override
    public boolean isDestroyed() {
        // TODO:
        // since this memory manager is multi-threaded,
        // currently there's no sync relation between #destroy() method
        // and other methods.
        return destroyed.get();
    }

    @Override
    protected int getQueueMergeThreshold(AddressQueue queue) {
        return INITIAL_CAPACITY;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"BC_IMPOSSIBLE_CAST", "BC_IMPOSSIBLE_INSTANCEOF"})
    private class GlobalAddressQueue implements AddressQueue, GarbageCollectable {

        private final int index;
        private final int memorySize;
        private final LongQueue queue;
        private final AtomicBoolean compactionFlag = new AtomicBoolean(false);

        private GlobalAddressQueue(int index, int memorySize) {
            this.index = index;
            this.memorySize = memorySize;
            this.queue = createQueue();
            registerGC();
        }

        private LongLinkedBlockingQueue createQueue() {
            return new LongLinkedBlockingQueue(systemAllocator, INVALID_ADDRESS);
        }

        private void registerGC() {
            if (queue instanceof GarbageCollectable) {
                gc.registerGarbageCollectable((GarbageCollectable) queue);
            }
            gc.registerGarbageCollectable(this);
        }

        @Override
        public boolean beforeCompaction() {
            return compactionFlag.compareAndSet(false, true);
        }

        @Override
        public void afterCompaction() {
            compactionFlag.set(false);
        }

        @Override
        public final long acquire() {
            return queue.poll();
        }

        @Override
        public final boolean release(long address) {
            if (address == INVALID_ADDRESS) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            return queue.offer(address);
        }

        public final int getMemorySize() {
            return memorySize;
        }

        public int capacity() {
            return queue.capacity();
        }

        public final int remaining() {
            return queue.size();
        }

        @Override
        public void destroy() {
            if (queue instanceof GarbageCollectable) {
                gc.deregisterGarbageCollectable((GarbageCollectable) queue);
            }
            gc.deregisterGarbageCollectable(this);
            queue.destroy();
        }

        @Override
        public void gc() {
            compact(this);
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("GlobalAddressQueue{");
            sb.append("memorySize=").append(MemorySize.toPrettyString(memorySize));
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        return "GlobalPoolingMemoryManager";
    }
}
