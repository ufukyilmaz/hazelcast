package com.hazelcast.memory;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.elastic.LongIterator;
import com.hazelcast.elastic.NativeSort;
import com.hazelcast.elastic.map.long2long.Long2LongElasticMap;
import com.hazelcast.elastic.map.long2long.Long2LongElasticMapHsa;
import com.hazelcast.elastic.map.long2long.LongLongCursor;
import com.hazelcast.elastic.queue.LongArrayQueue;
import com.hazelcast.elastic.set.LongHashSet;
import com.hazelcast.elastic.set.LongSet;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.nio.Bits;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;

@SuppressWarnings("checkstyle:methodcount")
public class ThreadLocalPoolingMemoryManager extends AbstractPoolingMemoryManager implements HazelcastMemoryManager {

    // Size of the memory block header in bytes
    private static final int HEADER_SIZE = 1;
    // Using sign bit as block is allocated externally from page allocator (OS) directly.
    // for allocations bigger than page size.
    private static final int EXTERNAL_BLOCK_BIT = Byte.SIZE - 1;
    // Using sign bit as available bit, since offset is already positive
    // so the first bit represents that is this memory block is available or not (in use already)
    private static final int AVAILABLE_BIT = EXTERNAL_BLOCK_BIT - 1;
    // The second bit represents that is this memory block has offset value to its owner page
    private static final int PAGE_OFFSET_EXIST_BIT = AVAILABLE_BIT - 1;
    // Extra required native memory when page offset is stored.
    // Since page offset should be aligned to 4 byte and header is 1 byte, we have 3 byte padding before header.
    private static final int MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED = Bits.INT_SIZE_IN_BYTES + Bits.INT_SIZE_IN_BYTES;

    // Initial capacity for various internal allocations such as page addresses, address queues, etc ...
    private static final int INITIAL_CAPACITY = 1024;
    // Load factor for the page allocation hash set
    private static final float LOAD_FACTOR = 0.60f;
    // Time interval in milliseconds to shrink address queues
    private static final long SHRINK_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    /*
     * Internal Memory Block Header Structure: (1 byte = 8 bits)
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Encoded size           |   5 bits             | ==> Size is encoded by logarithm of size
     * +------------------------+----------------------+
     * | PAGE_OFFSET_EXIST_BIT  |   1 bit              |
     * +------------------------+----------------------+
     * | AVAILABLE_BIT          |   1 bit              |
     * +------------------------+----------------------+
     * | EXTERNAL_BLOCK_BIT     |   1 bit              | ==> Set to 0 always
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: Size can be encoded by logarithm of size because regarding to buddy allocation algorithm,
     *      all sizes are power of 2.
     */

    /*
     * Internal Memory Block Structure:
     *
     * Headers are located at the end of previous record.
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +                    BLOCK N-1                  +
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | ...                    |                      |
     * +------------------------+----------------------+
     * | Header of BLOCK N      |   1 byte (byte)      |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +                     BLOCK N                   +
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Usable Memory Region   |   <size>             |
     * +------------------------+----------------------+
     * | Internal Fragmentation |                      |
     * +------------------------+----------------------+
     * | Page Offset            |   4 bytes (int)      | ==> If `PAGE_OFFSET_EXIST_BIT` is set in the header
     * +------------------------+----------------------+
     * | Padding                |   3 bytes            |
     * +------------------------+----------------------+
     * | Header of BLOCK N+1    |   1 byte             |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: If the memory block is the first block in its page,
     *      its header is located at the end of last memory block in the page.
     *      It means at the end of page.
     */

    /*
     * External Memory Block Header Structure: (1 byte = 8 bits)
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | <RESERVED>             |   7 bits             |
     * +------------------------+----------------------+
     * | EXTERNAL_BLOCK_BIT     |   1 bit              | ==> Set to 1 always
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */

    /*
     * External Memory Block Structure:
     *
     * Headers are located before the usable memory region.
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | <RESERVED>             |   7 bytes            |
     * +------------------------+----------------------+
     * | Header                 |   1 byte (byte)      |
     * +------------------------+----------------------+
     * | Usable Memory Region   |   <size>             |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */

    private final String threadName;
    private final LongSet pageAllocations;
    private final LongArray sortedPageAllocations;
    private final Long2LongElasticMap externalAllocations;
    private long lastFullCompaction;

    protected ThreadLocalPoolingMemoryManager(int minBlockSize, int pageSize,
                                              LibMalloc malloc, PooledNativeMemoryStats stats) {
        super(minBlockSize, pageSize, malloc, stats);
        pageAllocations = new LongHashSet(INITIAL_CAPACITY, LOAD_FACTOR, systemAllocator, NULL_ADDRESS);
        final MemoryManagerBean systemMemMgr = new MemoryManagerBean(systemAllocator, AMEM);
        sortedPageAllocations = new LongArray(systemMemMgr, INITIAL_CAPACITY);
        externalAllocations = new Long2LongElasticMapHsa(SIZE_INVALID, systemMemMgr);
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
        if (isPageBaseAddress(address)) {
            // If this is the first block, wrap around
            return address + pageSize - HEADER_SIZE;
        }
        return address - HEADER_SIZE;
    }

    private byte getHeader(long address) {
        long headerAddress = getHeaderAddress(address);
        return AMEM.getByte(headerAddress);
    }

    private static boolean isHeaderAvailable(byte header) {
        return Bits.isBitSet(header, AVAILABLE_BIT);
    }

    private boolean isAddressAvailable(long address) {
        byte header = getHeader(address);
        return isHeaderAvailable(header);
    }

    private static int getSizeFromHeader(byte header) {
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

    private static long getPageOffsetAddressBySize(long address, int size) {
        // We keep the page offset (if there is enough space) at the end of block as aligned
        return address + size - MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    }

    private boolean isPageBaseAddress(long address) {
        return pageAllocations.contains(address);
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
        AMEM.putByte(headerAddress, header);
        AMEM.putInt(address, offset);
    }

    @Override
    protected long allocateExternal(long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long address = pageAllocator.allocate(allocationSize);

        long existingAllocationSize = externalAllocations.putIfAbsent(address, allocationSize);
        if (existingAllocationSize != SIZE_INVALID) {
            pageAllocator.free(address, allocationSize);
            throw new AssertionError("Duplicate malloc() for external address " + address);
        }

        byte header = Bits.setBit((byte) 0, EXTERNAL_BLOCK_BIT);
        long internalHeaderAddress = address + EXTERNAL_BLOCK_HEADER_SIZE - HEADER_SIZE;
        AMEM.putByte(null, internalHeaderAddress, header);

        return address + EXTERNAL_BLOCK_HEADER_SIZE;
    }

    @Override
    protected void freeExternal(long address, long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long allocationAddress = address - EXTERNAL_BLOCK_HEADER_SIZE;

        long actualAllocationSize = externalAllocations.remove(allocationAddress);
        if (actualAllocationSize == SIZE_INVALID) {
            throw new AssertionError("Double free() -> external address: " + address + ", size: " + size);
        } else {
            long actualSize = actualAllocationSize - EXTERNAL_BLOCK_HEADER_SIZE;
            if (actualSize != size) {
                // Put it back
                externalAllocations.put(allocationAddress, actualAllocationSize);
                throw new AssertionError("Invalid size -> actual: " + actualSize + ", expected: " + size
                        + " while free external address " + address);
            } else {
                pageAllocator.free(allocationAddress, allocationSize);
            }
        }
    }

    @Override
    protected void markAvailable(long address) {
        assertNotNullPtr(address);

        long headerAddress = getHeaderAddress(address);
        byte header = AMEM.getByte(headerAddress);
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

        AMEM.putByte(headerAddress, availableHeader);
        AMEM.putInt(getPageOffsetAddressBySize(address, size), 0);
        AMEM.putInt(address, pageOffset);
    }

    @Override
    protected boolean markUnavailable(long address, int usedSize, int internalSize) {
        assertNotNullPtr(address);

        int offset = getOffset(address);
        long headerAddress = getHeaderAddressByOffset(address, offset);
        byte header = AMEM.getByte(headerAddress);
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

        AMEM.putByte(headerAddress, unavailableHeader);
        if (pageOffsetExist) {
            // If page offset will be stored, write it to the unused part of the memory block.
            AMEM.putInt(getPageOffsetAddressBySize(address, internalSize), offset);
        }
        AMEM.putInt(address, 0);

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
        AMEM.putByte(headerAddress, (byte) 0);
        AMEM.putInt(getPageOffsetAddressBySize(address, expectedSize), 0);
        AMEM.putInt(address, 0);

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

        return isPageBaseAddress(address - offset);
    }

    private long findSizeExternal(long address) {
        long allocationAddress = address - EXTERNAL_BLOCK_HEADER_SIZE;
        long allocationSize = externalAllocations.get(allocationAddress);
        if (allocationSize == SIZE_INVALID) {
            return SIZE_INVALID;
        } else {
            return allocationSize;
        }
    }

    private long findSize(long address, byte header) {
        if (Bits.isBitSet(header, EXTERNAL_BLOCK_BIT)) {
            return findSizeExternal(address);
        } else {
            return getSizeFromHeader(header);
        }
    }

    @Override
    protected long getSizeInternal(long address) {
        byte header = getHeader(address);
        return findSize(address, header);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        assertNotNullPtr(address);

        byte header = getHeader(address);
        long size = findSize(address, header);

        if (size > pageSize) {
            return size;
        } else {
            if (isHeaderAvailable(header) || !QuickMath.isPowerOfTwo(size) || size < minBlockSize) {
                return SIZE_INVALID;
            }
            long page = getOwningPage(address, header);
            return page != NULL_ADDRESS && page + pageSize >= address + size ? size : SIZE_INVALID;
        }
    }

    @Override
    protected int getOffset(long address) {
        return AMEM.getInt(address);
    }

    protected long getOwningPage(long address, byte header) {
        if (Bits.isBitSet(header, PAGE_OFFSET_EXIST_BIT)) {
            // If page offset is stored in the memory block, get the offset directly from there
            // and calculate page address by using this offset.

            int pageOffset = AMEM.getInt(getPageOffsetAddressByHeader(address, header));
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
                int middle = (low + high) >>> 1;
                long pageBase = sortedPageAllocations.get(middle);
                long pageEnd = pageBase + pageSize - 1;
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
    public boolean isDisposed() {
        return addressQueues[0] == null;
    }

    @Override
    public void dispose() {
        if (isDisposed()) {
            return;
        }
        for (int i = 0; i < addressQueues.length; i++) {
            AddressQueue q = addressQueues[i];
            if (q != null) {
                q.destroy();
                addressQueues[i] = null;
            }
        }
        disposePageAllocations();
        sortedPageAllocations.dispose();
        disposeExternalAllocations();
    }

    private void disposePageAllocations() {
        if (!pageAllocations.isEmpty()) {
            LongIterator iterator = pageAllocations.iterator();
            while (iterator.hasNext()) {
                long address = iterator.next();
                freePage(address);
                iterator.remove();
            }
        }
        pageAllocations.dispose();
    }

    private void disposeExternalAllocations() {
        if (!externalAllocations.isEmpty()) {
            for (LongLongCursor cursor = externalAllocations.cursor(); cursor.advance(); ) {
                long address = cursor.key();
                long size = cursor.value();
                pageAllocator.free(address, size);
            }
        }
        externalAllocations.dispose();
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

        private static final float RESIZE_CAPACITY_THRESHOLD = .75f;

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
        public long acquire() {
            if (queue != null) {
                shrink(false);
                return queue.poll();
            }
            return INVALID_ADDRESS;
        }

        private void shrink(boolean force) {
            int capacity = queue.capacity();
            if (capacity > INITIAL_CAPACITY && queue.remainingCapacity() > capacity * RESIZE_CAPACITY_THRESHOLD) {
                long now = Clock.currentTimeMillis();
                if (force || now > lastResize + SHRINK_INTERVAL) {
                    queue = resizeQueue(queue, queue.capacity() >> 1, true);
                    lastResize = now;
                }
            }
        }

        @Override
        public boolean release(long address) {
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
            return "ThreadAddressQueue{"
                    + "name=" + threadName
                    + ", memorySize=" + MemorySize.toPrettyString(memorySize)
                    + ", queue=" + queue
                    + '}';
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
