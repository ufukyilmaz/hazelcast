package com.hazelcast.memory;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.elastic.queue.LongArrayQueue;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.internal.util.collection.Long2LongMap;
import com.hazelcast.internal.util.collection.Long2LongMapHsa;
import com.hazelcast.internal.util.collection.LongCursor;
import com.hazelcast.internal.util.collection.LongLongCursor;
import com.hazelcast.internal.util.collection.LongSet;
import com.hazelcast.internal.util.collection.LongSetHsa;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.sort.LongMemArrayQuickSorter;
import com.hazelcast.internal.util.sort.MemArrayQuickSorter;
import com.hazelcast.nio.Bits;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;

/**
 * Single-threaded implementation of a pooling memory manager. To use it, a thread must register itself
 * by calling {@link PoolingMemoryManager#registerThread(Thread)}. After that, all requests from that thread
 * will be forwarded to its own {@code ThreadLocalPoolingMemoryManager} instance. A request to {@code free} a
 * block from a thread different than the one that {@code allocate}d it will result in an exception.
 */
@SuppressWarnings("checkstyle:methodcount")
public class ThreadLocalPoolingMemoryManager extends AbstractPoolingMemoryManager implements HazelcastMemoryManager {

    // Size of the memory block header in bytes
    static final int HEADER_SIZE = 1;
    // Extra required native memory when page offset is stored.
    // Since page offset should be aligned to 4 byte and header is 1 byte, we have 3 byte padding before header.
    static final int MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED = Bits.INT_SIZE_IN_BYTES + Bits.INT_SIZE_IN_BYTES;
    // AND-mask to retrieve the encoded block size
    @SuppressWarnings("checkstyle:magicnumber")
    static final int BLOCK_SIZE_MASK = (1 << 5) - 1;
    // The sign bit of the header signals that the block is allocated directly from the page allocator.
    // For allocations bigger than page size.
    static final int EXTERNAL_BLOCK_BIT = Byte.SIZE - 1;
    // Using sign bit as available bit, since offset is already positive
    // so the first bit represents that is this memory block is available or not (in use already)
    static final int AVAILABLE_BIT = EXTERNAL_BLOCK_BIT - 1;
    // The second bit represents that is this memory block has offset value to its owner page
    static final int PAGE_OFFSET_EXIST_BIT = AVAILABLE_BIT - 1;

    // Initial capacity for various internal allocations such as page addresses, address queues, etc ...
    private static final int INITIAL_CAPACITY = 1024;
    // Load factor for the page allocation hash set
    private static final float LOAD_FACTOR = 0.60f;
    // Time interval in milliseconds to shrink address queues
    private static final long SHRINK_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    /*
     * Internal Memory Block Header (1 byte = 8 bits, lowest bits shown at the top):
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Encoded size           |   5 bits             | ==> log2 of size
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
    private final Long2LongMap externalAllocations;
    private final MemArrayQuickSorter pageAllocationsSorter;
    private long lastFullCompaction;

    protected ThreadLocalPoolingMemoryManager(int minBlockSize, int pageSize,
                                              LibMalloc malloc, PooledNativeMemoryStats stats) {
        super(minBlockSize, pageSize, malloc, stats);
        final MemoryManagerBean systemMemMgr = new MemoryManagerBean(systemAllocator, AMEM);
        sortedPageAllocations = new LongArray(systemMemMgr, INITIAL_CAPACITY);
        pageAllocations = new LongSetHsa(NULL_ADDRESS, systemMemMgr, INITIAL_CAPACITY, LOAD_FACTOR);
        pageAllocationsSorter = new LongMemArrayQuickSorter(AMEM, NULL_ADDRESS);
        externalAllocations = new Long2LongMapHsa(SIZE_INVALID, systemMemMgr);
        initializeAddressQueues();
        threadName = Thread.currentThread().getName();
    }

    @Override
    public boolean isDisposed() {
        return addressQueues[0] == DestroyedAddressQueue.INSTANCE;
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
                addressQueues[i] = DestroyedAddressQueue.INSTANCE;
            }
        }
        disposePageAllocations();
        sortedPageAllocations.dispose();
        disposeExternalAllocations();
    }

    @Override
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public long validateAndGetAllocatedSize(long address) {
        assertValidAddress(address);

        final long pageBase = searchForOwningPage(address);
        if (pageBase == NULL_ADDRESS) {
            return findSizeOfExternalAllocation(address);
        }
        final int pageOffset = (int) (address - pageBase);
        final byte header = AMEM.getByte(toHeaderAddress(address, pageOffset));
        final int decodedSize = decodeSizeFromHeader(header);
        return !isHeaderAvailable(header)
                && !isExternalBlockHeader(header)
                && isLegalInternalBlockSize(decodedSize)
                && pageBase + pageSize >= address + decodedSize
                && (!hasStoredPageOffset(header) || getStoredPageOffset(address, decodedSize) == pageOffset)
            ? decodedSize : SIZE_INVALID;
    }

    @Override
    public String toString() {
        return "ThreadLocalPoolingMemoryManager [" + threadName + ']';
    }

    @Override
    protected void initialize(long address, int size, int offset) {
        assertValidAddress(address);
        assert QuickMath.isPowerOfTwo(size) : "Invalid size -> " + size + " is not power of two";
        assert size >= minBlockSize : "Invalid size -> "
                + size + " cannot be smaller than minimum block size " + minBlockSize;
        assert offset >= 0 : "Invalid offset -> " + offset + " is negative";

        byte header = createAvailableHeader(size);
        long headerAddress = toHeaderAddress(address, offset);
        AMEM.putByte(headerAddress, header);
        AMEM.putInt(address, offset);
    }

    @Override
    protected AddressQueue createAddressQueue(int index, int size) {
        return new ThreadAddressQueue(index, size);
    }

    @Override
    protected int headerSize() {
        return HEADER_SIZE;
    }

    @Override
    protected void onMallocPage(long pageAddress) {
        boolean added = pageAllocations.add(pageAddress);
        if (added) {
            try {
                addToSortedAllocations(pageAddress);
            } catch (NativeOutOfMemoryError e) {
                pageAllocations.remove(pageAddress);
                freePage(pageAddress);
                throw e;
            }
        }
        assert added : "Duplicate malloc() for page address " + pageAddress;
        lastFullCompaction = 0L;
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
    protected long allocateExternalBlock(long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long address = pageAllocator.allocate(allocationSize);

        long existingAllocationSize = externalAllocations.putIfAbsent(address, allocationSize);
        if (existingAllocationSize != SIZE_INVALID) {
            pageAllocator.free(address, allocationSize);
            throw new AssertionError(String.format(
                    "Page allocator returned an already allocated address %x. Existing size is %x, requested size is %x",
                    address, existingAllocationSize, size));
        }
        final byte header = markAsExternalBlockHeader((byte) 0);
        long internalHeaderAddress = address + EXTERNAL_BLOCK_HEADER_SIZE - HEADER_SIZE;
        AMEM.putByte(null, internalHeaderAddress, header);
        return address + EXTERNAL_BLOCK_HEADER_SIZE;
    }

    @Override
    protected void freeExternalBlock(long address, long size) {
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
                throw new AssertionError(String.format(
                        "The call free(%x, %x) refers to an externally allocated block, but the supplied size is wrong."
                        + " Actual block size is %x", address, size, actualSize
                ));
            } else {
                pageAllocator.free(allocationAddress, allocationSize);
            }
        }
    }

    @Override
    protected void markAvailable(long address) {
        assertValidAddress(address);

        final long headerAddress = toHeaderAddress(address);
        final byte header = AMEM.getByte(headerAddress);
        assert !isHeaderAvailable(header) : String.format(
                "Block header at address %x is corrupt because it is already marked as available", address);
        final int decodedSize = decodeSizeFromHeader(header);
        assert isLegalInternalBlockSize(decodedSize) : String.format(
                "Block header at address %x is corrupt because it encodes an invalid size %x", address, decodedSize);
        final int pageOffset;
        if (hasStoredPageOffset(header)) {
            pageOffset = getStoredPageOffset(address, decodedSize);
            assert isLegalPageOffsetAndSize(pageOffset, decodedSize)
                    : String.format("Block at address %x, decoded size %x, with decoded offset %x within the owning page,"
                    + " is corrupt because it cannot fit into a page of size %x",
                    address, decodedSize, pageOffset, pageSize);
            assert address > pageOffset : String.format(
                    "Block header at address %x of decoded size %x, with decoded offset %x within the owning page, is"
                    + " corrupt because the derived page base address is %x",
                    address, decodedSize, pageOffset, address - pageOffset);
        } else {
            final long pageBase = searchForOwningPage(address);
            assert pageBase > NULL_ADDRESS : String.format(
                    "Block at address %x of decoded size %x is corrupt because it doesn't fit into any allocated page",
                    address, decodedSize);
            assert address + decodedSize <= pageBase + pageSize
                    : String.format(
                    "Block [%x, %x] (decoded size %x) is corrupt because its header belongs to the page [%x, %x],"
                    + " but the rest of the block extends beyond the page end", address, address + decodedSize - 1,
                    decodedSize, pageBase, pageBase + pageSize - 1);
            pageOffset = (int) (address - pageBase);
        }
        final byte availableHeader = makeHeaderAvailable(header);

        AMEM.putByte(headerAddress, availableHeader);
        AMEM.putInt(addressOfStoredPageOffset(address, decodedSize), 0);
        AMEM.putInt(address, pageOffset);
    }

    @Override
    protected boolean markUnavailable(long address, int usedSize, int internalSize) {
        assertValidAddress(address);

        final boolean canStorePageOffset = internalSize - usedSize >= MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
        final int offset = getOffsetWithinPage(address);
        final long headerAddress = toHeaderAddress(address, offset);
        final byte headerNow = AMEM.getByte(headerAddress);
        final byte headerToBe = makeHeaderUnavailable(headerNow, canStorePageOffset);

        AMEM.putByte(headerAddress, headerToBe);
        if (canStorePageOffset) {
            AMEM.putInt(addressOfStoredPageOffset(address, internalSize), offset);
        }
        AMEM.putInt(address, 0);
        return true;
    }

    @Override
    protected boolean isAvailable(long address) {
        assertValidAddress(address);
        return isAddressAvailable(address);
    }

    @Override
    protected boolean markInvalid(long address, int expectedSize, int offset) {
        assertValidAddress(address);
        assert expectedSize == getSizeInternal(address)
                : "Invalid size -> actual: " + getSizeInternal(address) + ", expected: " + expectedSize;
        long headerAddress = toHeaderAddress(address, offset);

        AMEM.putByte(headerAddress, (byte) 0);
        AMEM.putInt(addressOfStoredPageOffset(address, expectedSize), 0);
        AMEM.putInt(address, 0);

        return true;
    }

    @Override
    protected boolean isValidAndAvailable(long address, int expectedSize) {
        assertValidAddress(address);

        byte header = AMEM.getByte(toHeaderAddress(address));
        boolean available = isHeaderAvailable(header);
        if (!available) {
            return false;
        }
        int size = decodeSizeFromHeader(header);
        if (size != expectedSize || size < minBlockSize) {
            return false;
        }
        int offset = getOffsetWithinPage(address);
        return offset >= 0 && QuickMath.modPowerOfTwo(offset, size) == 0 && isPageBaseAddress(address - offset);

    }

    @Override
    protected long getSizeInternal(long address) {
        byte header = AMEM.getByte(toHeaderAddress(address));
        return isExternalBlockHeader(header)
                ? findSizeOfExternalAllocation(address)
                : decodeSizeFromHeader(header);
    }

    @Override
    protected int getOffsetWithinPage(long address) {
        return AMEM.getInt(address);
    }

    @Override
    protected int getQueueMergeThreshold(AddressQueue queue) {
        return queue.capacity() / 3;
    }

    @Override
    protected Counter newCounter() {
        return new ThreadLocalCounter();
    }

    // package-private to support testing
    long toHeaderAddress(long address) {
        if (isPageBaseAddress(address)) {
            // If this is the first block, wrap around
            return address + pageSize - HEADER_SIZE;
        }
        return address - HEADER_SIZE;
    }

    private boolean isLegalInternalBlockSize(int size) {
        return size >= minBlockSize && size <= pageSize;
    }

    private boolean isLegalPageOffsetAndSize(int pageOffset, int size) {
        return pageOffset >= 0 && pageOffset + size <= pageSize;
    }

    private boolean isAddressAvailable(long address) {
        return isHeaderAvailable(AMEM.getByte(toHeaderAddress(address)));
    }

    private boolean isPageBaseAddress(long address) {
        return pageAllocations.contains(address);
    }

    private long findSizeOfExternalAllocation(long address) {
        return externalAllocations.get(address - EXTERNAL_BLOCK_HEADER_SIZE);
    }

    private void addToSortedAllocations(long address) {
        long len = pageAllocations.size();
        if (sortedPageAllocations.length() == len) {
            long newArrayLen = sortedPageAllocations.length() << 1;
            sortedPageAllocations.expand(newArrayLen);
        }
        sortedPageAllocations.set(len - 1, address);
        pageAllocationsSorter.gotoAddress(sortedPageAllocations.address()).sort(0, len);
    }

    private long searchForOwningPage(long address) {
        // Find page address by binary search in sorted page allocations list.
        long low = 0;
        long high = pageAllocations.size() - 1;

        while (low <= high) {
            long middle = (low + high) >>> 1;
            long pageBase = sortedPageAllocations.get(middle);
            long pageEnd = pageBase + pageSize - 1;
            if (address > pageEnd) {
                low = middle + 1;
            } else if (address < pageBase) {
                high = middle - 1;
            } else {
                assert pageBase > NULL_ADDRESS : String.format(
                        "sortedPageAllocations is corrupt because it contains address %x", pageBase);
                return pageBase;
            }
        }
        return NULL_ADDRESS;
    }

    private void disposePageAllocations() {
        if (!pageAllocations.isEmpty()) {
            for (LongCursor cursor = pageAllocations.cursor(); cursor.advance();) {
                freePage(cursor.value());
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

    private int getStoredPageOffset(long address, int size) {
        assert isLegalInternalBlockSize(size) : String.format(
                "Header of block at address %x, with decoded size %x, is corrupt because the decoded size"
                        + " is outside its legal range", address, size);
        return AMEM.getInt(addressOfStoredPageOffset(address, size));
    }

    // These static methods are used privately, but are package-private to support testing:

    static byte setHasStoredPageOffset(byte unavailableHeader) {
        return Bits.setBit(unavailableHeader, PAGE_OFFSET_EXIST_BIT);
    }

    static byte unsetHasStoredPageOffset(byte header) {
        return Bits.clearBit(header, PAGE_OFFSET_EXIST_BIT);
    }

    static byte encodeSize(int size) {
        return (byte) QuickMath.log2(size);
    }

    static byte createAvailableHeader(int size) {
        return Bits.setBit(encodeSize(size), AVAILABLE_BIT);
    }

    static byte makeHeaderAvailable(byte header) {
        return unsetHasStoredPageOffset(Bits.setBit(header, AVAILABLE_BIT));
    }

    static byte makeHeaderUnavailable(byte header, boolean canStorePageOffset) {
        final byte newHeader = canStorePageOffset ? setHasStoredPageOffset(header) : unsetHasStoredPageOffset(header);
        return Bits.clearBit(newHeader, AVAILABLE_BIT);
    }

    static byte markAsExternalBlockHeader(byte header) {
        return Bits.setBit(header, EXTERNAL_BLOCK_BIT);
    }

    // We keep the page offset (if there is enough space) at the end of the allocated block, aligned
    static long addressOfStoredPageOffset(long blockBase, int blockSize) {
        return blockBase + blockSize - MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    }


    private static boolean isHeaderAvailable(byte header) {
        return Bits.isBitSet(header, AVAILABLE_BIT);
    }

    private static boolean isExternalBlockHeader(byte header) {
        return Bits.isBitSet(header, EXTERNAL_BLOCK_BIT);
    }

    private static boolean hasStoredPageOffset(byte header) {
        return Bits.isBitSet(header, PAGE_OFFSET_EXIST_BIT);
    }

    private static int decodeSizeFromHeader(byte header) {
        return 1 << (header & BLOCK_SIZE_MASK);
    }

    private final class ThreadAddressQueue implements AddressQueue {

        private static final float RESIZE_CAPACITY_THRESHOLD = .75f;

        private final int index;
        private final int memorySize;
        private LongArrayQueue queue;
        private long lastGC;
        private long lastResize;

        ThreadAddressQueue(int index, int memorySize) {
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

    private static final class ThreadLocalCounter implements Counter {
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
}
