package com.hazelcast.memory;

import com.hazelcast.elasticcollections.LongIterator;
import com.hazelcast.elasticcollections.queue.LongArrayQueue;
import com.hazelcast.elasticcollections.set.LongHashSet;
import com.hazelcast.elasticcollections.set.LongSet;
import com.hazelcast.memory.error.NativeMemoryOutOfMemoryError;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.Bits;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 03/12/13
 */
final class ThreadLocalPoolingMemoryManager
        extends AbstractPoolingMemoryManager
        implements MemoryManager {

    private static final int HEADER_OFFSET = 1;
    // using sign bit as available bit, since offset is already positive
    private static final int AVAILABLE_BIT = Byte.SIZE - 1;
    private static final int INITIAL_CAPACITY = 1024;
    private static final long SHRINK_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    private final String threadName;
    private final LongSet allocations;
    private long lastFullCompaction;

    ThreadLocalPoolingMemoryManager(int minBlockSize, int pageSize,
            LibMalloc malloc, PooledNativeMemoryStats stats) {
        super(minBlockSize, pageSize, malloc, stats);
        allocations = new LongHashSet(INITIAL_CAPACITY, 0.91f, systemAllocator);
        initializeAddressQueues();
        threadName = Thread.currentThread().getName();
    }

    @Override
    protected AddressQueue createAddressQueue(int index, int size) {
        return new ThreadAddressQueue(index, size);
    }

    @Override
    protected int getHeaderSize() {
        return HEADER_OFFSET;
    }

    @Override
    protected void onMalloc(long address) {
        boolean added = allocations.add(address);
        assert added : "Duplicate malloc() for address: " + address;
        lastFullCompaction = 0L;
    }

    @Override
    protected void onFree(long address) {
        boolean removed = allocations.remove(address);
        assert removed : "Unknown address is freed: " + address;
        lastFullCompaction = 0L;
    }

    @Override
    protected void onOome(NativeMemoryOutOfMemoryError e) {
        long now = Clock.currentTimeMillis();
        if (now > lastFullCompaction + GarbageCollector.GC_INTERVAL) {
            compact();
            lastFullCompaction = now;
        }
    }

    @Override
    protected void initialize(long address, int size, int offset) {
        assertNotNullPtr(address);
        assert QuickMath.isPowerOfTwo(size) : "Invalid size: not power of two! " + size;
        assert offset >= 0 : "Invalid offset: negative! " + offset;

        byte h = (byte) QuickMath.log2(size);
        h = Bits.setBit(h, AVAILABLE_BIT);
        UnsafeHelper.UNSAFE.putByte(address, h);
        UnsafeHelper.UNSAFE.putInt(address + HEADER_OFFSET, offset);
    }

    @Override
    protected void markAvailable(long address) {
        assertNotNullPtr(address);

        byte b = UnsafeHelper.UNSAFE.getByte(address);
        assert !Bits.isBitSet(b, AVAILABLE_BIT) : "Address already marked as available! " + address;

        int memSize = (int) Math.pow(2, b);
        b = Bits.setBit(b, AVAILABLE_BIT);
        UnsafeHelper.UNSAFE.putByte(address, b);

        long base = getPage(address, memSize);
        if (base < 0) {
            throw new IllegalArgumentException("Address: " + address + " does not belong to this memory pool!");
        }
        int offset = (int) (address - base);
        assert offset >= 0 : "Invalid offset: " + offset;
        UnsafeHelper.UNSAFE.putInt(address + HEADER_OFFSET, offset);
    }

    @Override
    protected boolean markUnavailable(long address) {
        assertNotNullPtr(address);
        byte b = UnsafeHelper.UNSAFE.getByte(address);
        b = Bits.clearBit(b, AVAILABLE_BIT);
        UnsafeHelper.UNSAFE.putByte(address, b);
        UnsafeHelper.UNSAFE.putInt(address + HEADER_OFFSET, 0);
        return true;
    }

    @Override
    protected boolean isAvailable(long address) {
        assertNotNullPtr(address);
        byte b = UnsafeHelper.UNSAFE.getByte(address);
        return Bits.isBitSet(b, AVAILABLE_BIT);
    }

    @Override
    protected boolean markInvalid(long address, int expectedSize) {
        assertNotNullPtr(address);
        assert expectedSize == getSize(address)
                : "Invalid size! actual: " + getSize(address) + ", expected: " + expectedSize;
        UnsafeHelper.UNSAFE.putByte(address, (byte) 0);
        UnsafeHelper.UNSAFE.putInt(address + HEADER_OFFSET, 0);
        return true;
    }

    @Override
    protected boolean isValidAndAvailable(long address, int expectedSize) {
        assertNotNullPtr(address);
        byte b = UnsafeHelper.UNSAFE.getByte(address);
        boolean available = Bits.isBitSet(b, AVAILABLE_BIT);
        if (!available) {
            return false;
        }

        b = Bits.clearBit(b, AVAILABLE_BIT);
        if (b < minBlockSizePower) {
            return false;
        }
        int memSize = (int) Math.pow(2, b);
        if (memSize != expectedSize) {
            return false;
        }

        int offset = getOffset(address);
        if (offset < 0 || QuickMath.modPowerOfTwo(offset, memSize) != 0) {
            return false;
        }
        return allocations.contains(address - offset);
    }

    @Override
    protected int getSize(long address) {
        byte b = UnsafeHelper.UNSAFE.getByte(address);
        b = Bits.clearBit(b, AVAILABLE_BIT);
        return 1 << b;
    }

    protected int getOffset(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + HEADER_OFFSET);
    }

    @Override
    public int getHeaderLength() {
        return HEADER_OFFSET;
    }

    @Override
    public long getPage(long address) {
        int size = getSize(address);
        return getPage(address, size);
    }

    private long getPage(long address, int size) {
        LongIterator iterator = allocations.iterator();
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

    @Override
    public void destroy() {
        for (int i = 0; i < addressQueues.length; i++) {
            AddressQueue q = addressQueues[i];
            if (q != null) {
                q.destroy();
                addressQueues[i] = null;
            }
        }
        if (!allocations.isEmpty()) {
            LongIterator iterator = allocations.iterator();
            while (iterator.hasNext()) {
                long address = iterator.next();
                pageAllocator.free(address, pageSize);
            }
        }
        allocations.destroy();
    }

    @Override
    protected int getQueueMergeThreshold(AddressQueue queue) {
        return queue.capacity() / 3;
    }

    private final class ThreadAddressQueue implements AddressQueue {

        private final int index;
        private final int memorySize;
        private LongArrayQueue queue;
        private long lastGC;
        private long lastResize;

        public ThreadAddressQueue(int index, int memorySize) {
            if (memorySize <= 0) {
                throw new IllegalArgumentException();
            }
            this.index = index;
            this.memorySize = memorySize;
        }

        @Override
        public boolean beforeCompaction() {
            return true;
        }

        @Override
        public void afterCompaction() {
        }

        public final long acquire() {
            if (queue != null) {
                shrink(false);
                return queue.poll();
            }
            return INVALID_ADDRESS;
        }

        private void shrink(boolean force) {
            int capacity = queue.capacity();
            if (capacity > INITIAL_CAPACITY && queue.remainingCapacity() > capacity * .75f) {
                long now = Clock.currentTimeMillis();
                if (force || now > lastResize + SHRINK_INTERVAL) {
                    queue = resizeQueue(queue, queue.capacity() >> 1, true);
                    lastResize = now;
                }
            }
        }

        public final boolean release(long address) {
            if (address == INVALID_ADDRESS) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            if (queue == null) {
                queue = resizeQueue(null, INITIAL_CAPACITY, true);
                lastResize = Clock.currentTimeMillis();
            } else if (queue.remainingCapacity() == 0) {
                long now = Clock.currentTimeMillis();
                if (now > lastGC + GarbageCollector.GC_INTERVAL) {
                    compact(this);
                    lastGC = now;
                }
                if (queue.remainingCapacity() == 0) {
                    queue = resizeQueue(queue, queue.capacity() << 1, true);
                    lastResize = now;
                }
            }

            boolean offered = queue.offer(address);
            assert offered : "Cannot offer!";
            return true;
        }

        private LongArrayQueue resizeQueue(LongArrayQueue current, int newCap, boolean purge) {
            LongArrayQueue queue;
            try {
                if (current != null) {
                    queue = new LongArrayQueue(systemAllocator, newCap, current);
                    current.destroy();
                } else {
                    queue = new LongArrayQueue(systemAllocator, newCap, INVALID_ADDRESS);
                }
            } catch (NativeMemoryOutOfMemoryError e) {
                if (purge) {
                    return purgeEmptySpaceAndResizeQueue(current, newCap);
                }
                throw new NativeMemoryOutOfMemoryError("Cannot expand internal memory pool -> " + e.getMessage(), e);
            }
            return queue;
        }

        private LongArrayQueue purgeEmptySpaceAndResizeQueue(LongArrayQueue current, int newCap) {
            purgeEmptySpace();
            return resizeQueue(current, newCap, false);
        }

        private void purgeEmptySpace() {
            for (AddressQueue addressQueue : addressQueues) {
                ThreadAddressQueue q = (ThreadAddressQueue) addressQueue;
                if (q != this) {
                    if (q.remaining() == 0) {
                        q.destroy();
                    } else {
                        q.shrink(true);
                    }
                }
            }
        }

        public int remaining() {
            return queue != null ? queue.size() : 0;
        }

        public int capacity() {
            return queue != null ? queue.capacity() : INITIAL_CAPACITY;
        }

        @Override
        public void destroy() {
            if (queue == null) {
                return;
            }
            queue.destroy();
            queue = null;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ThreadAddressQueue{");
            sb.append("name=").append(threadName);
            sb.append(", memorySize=").append(MemorySize.toPrettyString(memorySize));
            sb.append(", queue=").append(queue);
            sb.append('}');
            return sb.toString();
        }

        public int getMemorySize() {
            return memorySize;
        }
    }

    @Override
    public String toString() {
        return "ThreadLocalPoolingMemoryManager [" + threadName + "]";
    }
}
