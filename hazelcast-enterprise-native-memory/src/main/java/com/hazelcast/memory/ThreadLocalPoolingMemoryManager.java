package com.hazelcast.memory;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.elastic.LongIterator;
import com.hazelcast.elastic.NativeSort;
import com.hazelcast.elastic.queue.LongArrayQueue;
import com.hazelcast.elastic.set.LongHashSet;
import com.hazelcast.elastic.set.LongSet;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
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
    private final LongSet pageAllocations;
    private final LongArray sortedPageAllocations;
    private long lastFullCompaction;

    ThreadLocalPoolingMemoryManager(int minBlockSize, int pageSize,
            LibMalloc malloc, PooledNativeMemoryStats stats) {
        super(minBlockSize, pageSize, malloc, stats);
        pageAllocations = new LongHashSet(INITIAL_CAPACITY, 0.91f, systemAllocator, NULL_ADDRESS);
        sortedPageAllocations = new LongArray(systemAllocator, INITIAL_CAPACITY);
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
    protected void onMallocPage(long pageAddress) {
        boolean added = pageAllocations.add(pageAddress);
        if (added) {
            try {
                addSorted(pageAddress);
            } catch (NativeOutOfMemoryError e) {
                pageAllocations.remove(pageAddress);
                freePage(pageAddress);
                throw e;
            }
        }
        assert added : "Duplicate malloc() for pageAddress: " + pageAddress;
        lastFullCompaction = 0L;
    }

    private void addSorted(long address) {
        int len = pageAllocations.size();
        if (sortedPageAllocations.length() == len) {
            long newArrayLen = sortedPageAllocations.length() << 1;
            sortedPageAllocations.expand(newArrayLen);
        }
        sortedPageAllocations.set(len - 1, address);
        NativeSort.quickSortLong(sortedPageAllocations.address(), len);
    }

    @Override
    protected void onOome(NativeOutOfMemoryError e) {
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
        if (base == NULL_ADDRESS) {
            throw new IllegalArgumentException("Address: " + address + " does not belong to this memory pool!");
        }
        int offset = (int) (address - base);
        assert offset >= 0 : "Invalid offset: " + offset;
        UnsafeHelper.UNSAFE.putInt(address + HEADER_OFFSET, offset);
    }

    @Override
    protected boolean markUnavailable(long address, int expectedSize) {
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
        assert expectedSize == getSizeInternal(address)
                : "Invalid size! actual: " + getSizeInternal(address) + ", expected: " + expectedSize;
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
        return pageAllocations.contains(address - offset);
    }

    @Override
    protected int getSizeInternal(long address) {
        byte b = UnsafeHelper.UNSAFE.getByte(address);
        b = Bits.clearBit(b, AVAILABLE_BIT);
        return 1 << b;
    }

    @Override
    public int getSize(long address) {
        return getSizeInternal(address - getHeaderSize());
    }

    @Override
    protected int getOffset(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + HEADER_OFFSET);
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

    // binary range search
    private long getPage(long address, long memSize) {
        final long blockEnd = address + memSize;
        int low = 0;
        int high = pageAllocations.size() - 1;

        while (low <= high) {
            int middle = (low + high) >>> 1;
            long pageAddress = sortedPageAllocations.get(middle);

            if (pageAddress <= address && (pageAddress + pageSize) >= blockEnd) {
                return pageAddress;
            }

            if (pageAddress <= address - pageSize) {
                low = middle + 1;
            } else if (pageAddress > address) {
                high = middle - 1;
            }
        }
        return NULL_ADDRESS;
    }

    @Override
    public boolean isDestroyed() {
        return addressQueues[0] == null;
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
        if (!pageAllocations.isEmpty()) {
            LongIterator iterator = pageAllocations.iterator();
            while (iterator.hasNext()) {
                long address = iterator.next();
                freePage(address);
            }
        }
        pageAllocations.dispose();
        sortedPageAllocations.dispose();
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

        @Override
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

        @Override
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
                    current.dispose();
                } else {
                    queue = new LongArrayQueue(systemAllocator, newCap, INVALID_ADDRESS);
                }
            } catch (NativeOutOfMemoryError e) {
                if (purge) {
                    return purgeEmptySpaceAndResizeQueue(current, newCap);
                }
                throw new NativeOutOfMemoryError("Cannot expand internal memory pool -> " + e.getMessage(), e);
            }
            return queue;
        }

        private LongArrayQueue purgeEmptySpaceAndResizeQueue(LongArrayQueue current, int newCap) {
            compact();
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

        @Override
        public int remaining() {
            return queue != null ? queue.size() : 0;
        }

        @Override
        public int capacity() {
            return queue != null ? queue.capacity() : INITIAL_CAPACITY;
        }

        @Override
        public void destroy() {
            if (queue == null) {
                return;
            }
            queue.dispose();
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
