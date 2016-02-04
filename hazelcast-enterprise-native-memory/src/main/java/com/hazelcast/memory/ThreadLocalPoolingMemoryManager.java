package com.hazelcast.memory;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.elastic.LongIterator;
import com.hazelcast.elastic.NativeSort;
import com.hazelcast.elastic.queue.LongArrayQueue;
import com.hazelcast.elastic.set.LongHashSet;
import com.hazelcast.elastic.set.LongSet;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 03/12/13
 */
public class ThreadLocalPoolingMemoryManager
        extends AbstractPoolingMemoryManager
        implements MemoryManager {

    // Size of the memory block header in bytes
    private static final int HEADER_SIZE = 1;
    // Using sign bit as available bit, since offset is already positive
    // so the first bit represents that is this memory block is available or not (in use already)
    private static final int AVAILABLE_BIT = Byte.SIZE - 1;
    // The second bit represents that is this memory block has offset value to its owner page
    private static final int PAGE_OFFSET_EXIST_BIT = AVAILABLE_BIT - 1;
    // Extra required native memory when page offset is stored.
    // Since page offset should be aligned to 4 byte and header is 1 byte, we have 3 byte padding before header.
    private static final int MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED = Bits.INT_SIZE_IN_BYTES + Bits.INT_SIZE_IN_BYTES;
    private static final int INITIAL_CAPACITY = 1024;
    private static final long SHRINK_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    /*
     * Header Structure: (1 byte = 8 bits)
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Encoded size           |   5 bits             | ==> Size is encoded by logarithm of size
     * +------------------------+----------------------+
     * | RESERVED               |   1 bit              |
     * +------------------------+----------------------+
     * | PAGE_OFFSET_EXIST_BIT  |   1 bit              |
     * +------------------------+----------------------+
     * | AVAILABLE_BIT          |   1 bit              |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: Size can be encoded by logarithm of size because regarding to buddy allocation algorithm,
     *      all sizes are power of 2.
     */

    /*
     * Memory Block Structure:
     *
     * Headers are located at the end of previous record.
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +                  RECORD N-1                   +
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | ...                    |                      |
     * +------------------------+----------------------+
     * | Header                 |   1 byte (byte)      |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +                   RECORD N                    +
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Used Memory            |   <size>             |
     * +------------------------+----------------------+
     * | Internal Fragmentation |                      |
     * +------------------------+----------------------+
     * | Page Offset            |   4 bytes (int)      | ==> (If `PAGE_OFFSET_EXIST_BIT` is set in the header)
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: If the memory block is the first block in its page,
     *      its header is located at the end of last memory block in the page.
     *      It means at the end of page.
     */

    private final String threadName;
    private final LongSet pageAllocations;
    private final LongArray sortedPageAllocations;
    private long lastFullCompaction;

    protected ThreadLocalPoolingMemoryManager(int minBlockSize, int pageSize,
                                              LibMalloc malloc, PooledNativeMemoryStats stats) {
        super(minBlockSize, pageSize, malloc, stats);
        pageAllocations = new LongHashSet(INITIAL_CAPACITY, 0.91f, systemAllocator, NULL_ADDRESS);
        sortedPageAllocations = new LongArray(systemAllocator, INITIAL_CAPACITY);
        initializeAddressQueues();
        threadName = Thread.currentThread().getName();
    }

    private static byte encodeSize(int size) {
        return (byte) QuickMath.log2(size);
    }

    private static int decodeSize(byte size) {
        return 1 << size;
    }

    private static byte initHeader(long address, int size, int offset) {
        return Bits.setBit(encodeSize(size), AVAILABLE_BIT);
    }

    private long getHeaderAddress(long address) {
        if (pageAllocations.contains(address)) {
            // If this is the first block, wrap around
            return address + pageSize - HEADER_SIZE;
        }
        return address - HEADER_SIZE;
    }

    private long getHeaderAddressByOffset(long address, int offset) {
        if (offset == 0) {
            // If this is the first block, wrap around
            return address + pageSize - HEADER_SIZE;
        }
        return address - HEADER_SIZE;
    }

    private byte getHeader(long address) {
        long headerAddress = getHeaderAddress(address);
        return UnsafeHelper.UNSAFE.getByte(headerAddress);
    }

    private boolean isHeaderAvailable(byte header) {
        return Bits.isBitSet(header, AVAILABLE_BIT);
    }

    private boolean isAddressAvailable(long address) {
        byte header = getHeader(address);
        return isHeaderAvailable(header);
    }

    private int getSizeFromHeader(byte header) {
        header = Bits.clearBit(header, AVAILABLE_BIT);
        header = Bits.clearBit(header, PAGE_OFFSET_EXIST_BIT);
        return decodeSize(header);
    }

    private int getSizeFromAddress(long address) {
        byte header = getHeader(address);
        return getSizeFromHeader(header);
    }

    private static byte makeHeaderAvailable(byte header) {
        return Bits.setBit(header, AVAILABLE_BIT);
    }

    private static byte makeHeaderUnavailable(byte header) {
        return Bits.clearBit(header, AVAILABLE_BIT);
    }

    private long getPageOffsetAddressByHeader(long address, byte header) {
        int size = getSizeFromHeader(header);
        // We keep the page offset (if there is enough space) at the end of block as aligned
        return address + size - MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    }

    private long getPageOffsetAddressBySize(long address, int size) {
        // We keep the page offset (if there is enough space) at the end of block as aligned
        return address + size - MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    }

    @Override
    protected AddressQueue createAddressQueue(int index, int size) {
        return new ThreadAddressQueue(index, size);
    }

    @Override
    protected int getHeaderSize() {
        return HEADER_SIZE;
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
        assert added : "Duplicate malloc() for page address " + pageAddress;
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
        assert QuickMath.isPowerOfTwo(size) : "Invalid size -> " + size + " is not power of two";
        assert size >= minBlockSize : "Invalid size -> "
                + size + " cannot be smaller than minimum block size " + minBlockSize;
        assert offset >= 0 : "Invalid offset -> " + offset + " is negative";

        byte header = initHeader(address, size, offset);
        long headerAddress = getHeaderAddressByOffset(address, offset);
        UnsafeHelper.UNSAFE.putByte(headerAddress, header);
        UnsafeHelper.UNSAFE.putInt(address, offset);
    }

    @Override
    protected void markAvailable(long address) {
        assertNotNullPtr(address);

        long headerAddress = getHeaderAddress(address);
        byte header = UnsafeHelper.UNSAFE.getByte(headerAddress);
        assert !isHeaderAvailable(header) : "Address " + address + " has been already marked as available!";

        long pageBase = getOwningPage(address, header);
        if (pageBase == NULL_ADDRESS) {
            throw new IllegalArgumentException("Address " + address + " does not belong to this memory pool!");
        }
        int size = getSizeFromHeader(header);
        assert pageBase <= address && pageBase + pageSize >= address + size
                : String.format("Block [%,d-%,d] partially overlaps page [%,d-%,d]",
                                address, address + size - 1,
                                pageBase, pageBase + pageSize - 1);

        int pageOffset = (int) (address - pageBase);
        assert pageOffset >= 0 : "Invalid offset -> " + pageOffset + " is negative!";

        byte availableHeader = makeHeaderAvailable(header);
        availableHeader = Bits.clearBit(availableHeader, PAGE_OFFSET_EXIST_BIT);

        UnsafeHelper.UNSAFE.putByte(headerAddress, availableHeader);
        UnsafeHelper.UNSAFE.putInt(getPageOffsetAddressBySize(address, size), 0);
        UnsafeHelper.UNSAFE.putInt(address, pageOffset);
    }

    @Override
    protected boolean markUnavailable(long address, int usedSize, int internalSize) {
        assertNotNullPtr(address);

        int offset = getOffset(address);
        long headerAddress = getHeaderAddressByOffset(address, offset);
        byte header = UnsafeHelper.UNSAFE.getByte(headerAddress);
        byte unavailableHeader = makeHeaderUnavailable(header);

        boolean pageOffsetExist = false;
        if (internalSize - usedSize >= MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED) {
            // If there is enough space for storing page offset,
            // set the header as page offset is stored in the memory block.
            unavailableHeader = Bits.setBit(unavailableHeader, PAGE_OFFSET_EXIST_BIT);
            pageOffsetExist = true;
        } else {
            unavailableHeader = Bits.clearBit(unavailableHeader, PAGE_OFFSET_EXIST_BIT);
        }

        UnsafeHelper.UNSAFE.putByte(headerAddress, unavailableHeader);
        if (pageOffsetExist) {
            // If page offset will be stored, write it to the unused part of the memory block.
            UnsafeHelper.UNSAFE.putInt(getPageOffsetAddressBySize(address, internalSize), offset);
        }
        UnsafeHelper.UNSAFE.putInt(address, 0);

        return true;
    }

    @Override
    protected boolean isAvailable(long address) {
        assertNotNullPtr(address);

        return isAddressAvailable(address);
    }

    @Override
    protected boolean markInvalid(long address, int expectedSize, int offset) {
        assertNotNullPtr(address);
        assert expectedSize == getSizeInternal(address)
                : "Invalid size -> actual: " + getSizeInternal(address) + ", expected: " + expectedSize;

        long headerAddress = getHeaderAddressByOffset(address, offset);
        UnsafeHelper.UNSAFE.putByte(headerAddress, (byte) 0);
        UnsafeHelper.UNSAFE.putInt(getPageOffsetAddressBySize(address, expectedSize), 0);
        UnsafeHelper.UNSAFE.putInt(address, 0);

        return true;
    }

    @Override
    protected boolean isValidAndAvailable(long address, int expectedSize) {
        assertNotNullPtr(address);

        byte header = getHeader(address);

        boolean available = isHeaderAvailable(header);
        if (!available) {
            return false;
        }

        int size = getSizeFromHeader(header);
        if (size != expectedSize || size < minBlockSizePower) {
            return false;
        }

        int offset = getOffset(address);
        if (offset < 0 || QuickMath.modPowerOfTwo(offset, size) != 0) {
            return false;
        }

        return pageAllocations.contains(address - offset);
    }

    @Override
    protected int getSizeInternal(long address) {
        return getSizeFromAddress(address);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        assertNotNullPtr(address);

        byte header = getHeader(address);
        int size = getSizeFromHeader(header);

        if (isHeaderAvailable(header) || !QuickMath.isPowerOfTwo(size)
                || size < minBlockSize || size > pageSize) {
            return SIZE_INVALID;
        }

        long page = getOwningPage(address, header);
        return page != NULL_ADDRESS && page + pageSize >= address + size ? size : SIZE_INVALID;
    }

    @Override
    protected int getOffset(long address) {
        return UnsafeHelper.UNSAFE.getInt(address);
    }

    protected long getOwningPage(long address, byte header) {
        if (Bits.isBitSet(header, PAGE_OFFSET_EXIST_BIT)) {
            // If page offset is stored in the memory block, get the offset directly from there
            // and calculate page address by using this offset.

            int pageOffset = UnsafeHelper.UNSAFE.getInt(getPageOffsetAddressByHeader(address, header));
            if (pageOffset < 0 || pageOffset > (pageSize - minBlockSize)) {
                throw new IllegalArgumentException("Invalid page offset for address " + address + ": " + pageOffset
                        + ". Because page offset cannot be `< 0` or `> (pageSize - minBlockSize)`"
                        + " where pageSize is " + pageSize + " and minBlockSize is " + minBlockSize);
            }
            return address - pageOffset;
        } else {
            // Otherwise, find page address by binary search in sorted page allocations list.
            int low = 0;
            int high = pageAllocations.size() - 1;

            while (low <= high) {
                final int middle = (low + high) >>> 1;
                final long pageBase = sortedPageAllocations.get(middle);
                final long pageEnd = pageBase + pageSize - 1;
                if (address > pageEnd) {
                    low = middle + 1;
                } else if (address < pageBase) {
                    high = middle - 1;
                } else {
                    return pageBase;
                }
            }
            return NULL_ADDRESS;
        }
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

    @Override
    protected Counter newCounter() {
        return new ThreadLocalCounter();
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
                /*
                 * While resizing queue, current address queue might be not-null but disposed.
                 * Because global compaction (`compact`) might be called
                 * by `purgeEmptySpaceAndResizeQueue` below and
                 * the sub-sequent calls through other address queues from here
                 * might dispose this address queue after compaction/merging buddies.
                 * So we need to check that is current address queue is still available or not.
                 */
                if (current != null && current.isAvailable()) {
                    queue = new LongArrayQueue(systemAllocator, newCap, current);
                    current.dispose();
                } else {
                    queue = new LongArrayQueue(systemAllocator, newCap, INVALID_ADDRESS);
                }
            } catch (NativeOutOfMemoryError e) {
                if (purge) {
                    try {
                        return purgeEmptySpaceAndResizeQueue(current, newCap);
                    } catch (OutOfMemoryError oome) {
                        OutOfMemoryErrorDispatcher.onOutOfMemory(oome);
                    } catch (NativeOutOfMemoryError oome) {
                        throw oome;
                    } catch (Throwable t) {
                        // We are printing actual exception's message and using `NativeOutOfMemoryError` as cause
                        throw new NativeOutOfMemoryError("Cannot expand internal memory pool "
                                + "even though purging and compacting are applied -> " + t.getMessage(), e);
                    }
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

    private static class ThreadLocalCounter implements Counter {
        private long value;

        @Override
        public long get() {
            return value;
        }

        @Override
        public long inc() {
            return ++value;
        }

        @Override
        public long inc(long amount) {
            value += amount;
            return value;
        }
    }

    @Override
    public String toString() {
        return "ThreadLocalPoolingMemoryManager [" + threadName + ']';
    }
}
