package com.hazelcast.memory;

import com.hazelcast.elastic.queue.LongLinkedBlockingQueue;
import com.hazelcast.elastic.queue.LongQueue;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mdogan 03/12/13
 */
final class GlobalPoolingMemoryManager
        extends AbstractPoolingMemoryManager
        implements MemoryManager {

    // Size of the memory block header in bytes
    private static final int HEADER_SIZE = 4;
    // Using sign bit as block is allocated externally from page allocator (OS) directly.
    // for allocations bigger than page size.
    private static final int EXTERNAL_BLOCK_BIT = Integer.SIZE - 1;
    // Using sign bit as available bit, since offset is already positive
    // so the first bit represents that is this memory block is available or not (in use already)
    private static final int AVAILABLE_BIT = EXTERNAL_BLOCK_BIT - 1;
    // The second bit represents that is this memory block has offset value to its owner page
    private static final int PAGE_OFFSET_EXIST_BIT = AVAILABLE_BIT - 1;
    // Extra required native memory when page offset is stored
    private static final int MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED = HEADER_SIZE + Bits.INT_SIZE_IN_BYTES;
    // We store size as encoded by shifting it 3 bit since it is always multiply of 8.
    private static final int SIZE_SHIFT_COUNT = 3;

    // Initial capacity for various internal allocations such as page addresses, address queues, etc ...
    private static final int INITIAL_CAPACITY = 2048;

    /*
     * External Memory Block Header Structure: (4 byte = 32 bits)
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Encoded size           |   28 bits            | ==> Size is encoded by shifting 3 bit
     * +------------------------+----------------------+
     * | <RESERVED>             |   1 bit              |
     * +------------------------+----------------------+
     * | PAGE_OFFSET_EXIST_BIT  |   1 bit              |
     * +------------------------+----------------------+
     * | AVAILABLE_BIT          |   1 bit              |
     * +------------------------+----------------------+
     * | EXTERNAL_BLOCK_BIT     |   1 bit              | ==> Set to 0 always
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: Size can be encoded by shifting 3 bit because regarding to buddy allocation algorithm,
     *      all sizes are power of 2 and there is minimum 16 byte so size must be multiply of 8.
     */

    /*
     * Internal Memory Block Structure:
     *
     * Headers are located at the end of previous record.
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * +                     BLOCK N-1                 +
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | ...                    |                      |
     * +------------------------+----------------------+
     * | Header of BLOCK N      |   4 bytes (int)      |
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
     * | Header of BLOCK N+1    |   4 bytes (int)      |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: If the memory block is the first block in its page,
     *      its header is located at the end of last memory block in the page.
     *      It means at the end of page.
     */

    /*
     * External Memory Block Header Structure: (4 byte = 32 bits)
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | LSB 31 bits of size    |   31 bits            |
     * +------------------------+----------------------+
     * | EXTERNAL_BLOCK_BIT     |   1 bit              | ==> Set to 1 always
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: The least significant 31 bits of the size are embedded into internal block header.
     */

    /*
     * External Memory Block Structure:
     *
     * Headers are located before the usable memory region.
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | MSB 32 bits of size    |   4 bytes (int)      |
     * +------------------------+----------------------+
     * | Header                 |   4 bytes (int)      | ==> Includes LSB 31 bits of size as mentioned above
     * +------------------------+----------------------+
     * | Usable Memory Region   |   <size>             |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * P.S: The most significant 32 bits of the size are embedded into external block header.
     *      The remaining most significant 1 bit is not used.
     *      Because since size is always positive that bit is always 0.
     */

    private final GarbageCollector gc;
    private final ConcurrentNavigableMap<Long, Object> pageAllocations
            = new ConcurrentSkipListMap<Long, Object>();
    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    private volatile long lastFullCompaction;

    GlobalPoolingMemoryManager(int minBlockSize, int pageSize,
            LibMalloc malloc, PooledNativeMemoryStats stats, GarbageCollector gc) {
        super(minBlockSize, pageSize, malloc, stats);
        this.gc = gc;
        initializeAddressQueues();
    }

    private static int encodeSize(int size) {
        // We may use `QuickMath.log2(size);` but it is more expensive than bit shifting.
        return size >> SIZE_SHIFT_COUNT;
    }

    private static int decodeSize(int size) {
        // We may use `1 << size` if we use `QuickMath.log2(size);` for encoding above.
        return size << SIZE_SHIFT_COUNT;
    }

    private int initHeader(int size) {
        return Bits.setBit(encodeSize(size), AVAILABLE_BIT);
    }

    private long getHeaderAddress(long address) {
        if (lookupPage(address)) {
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

    private int getHeader(long address) {
        long headerAddress = getHeaderAddress(address);
        return UnsafeHelper.UNSAFE.getIntVolatile(null, headerAddress);
    }

    private boolean isHeaderAvailable(int header) {
        return Bits.isBitSet(header, AVAILABLE_BIT);
    }

    private boolean isAddressAvailable(long address) {
        int header = getHeader(address);
        return isHeaderAvailable(header);
    }

    private int getSizeFromHeader(int header) {
        int size;
        size = Bits.clearBit(header, AVAILABLE_BIT);
        size = Bits.clearBit(size, PAGE_OFFSET_EXIST_BIT);
        return decodeSize(size);
    }

    private int getSizeFromAddress(long address) {
        int header = getHeader(address);
        return getSizeFromHeader(header);
    }

    private static int makeHeaderAvailable(int header) {
        return Bits.setBit(header, AVAILABLE_BIT);
    }

    private static int makeHeaderUnavailable(int header) {
        return Bits.clearBit(header, AVAILABLE_BIT);
    }

    private long getPageOffsetAddressByHeader(long address, int header) {
        int size = getSizeFromHeader(header);
        // We keep the page offset (if there is enough space) at the end of block as aligned
        return address + size - MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    }

    private long getPageOffsetAddressBySize(long address, int size) {
        // We keep the page offset (if there is enough space) at the end of block as aligned
        return address + size - MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED;
    }

    private boolean lookupPage(long address) {
        if (pageLookupAddress == NULL_ADDRESS) {
            return pageAllocations.containsKey(address);
        }
        // Get related bits and skip LSB 3 bits because every address is 8 bytes aligned.
        int idx = (int) (address & PAGE_LOOKUP_MASK) >> 3;
        // Find the 4 bytes block contains related lookup bit of given address.
        int lookupIndex = (idx >> 3) & 0xFFFFFFFC;
        // Find the bit offset in the found 4 bytes block.
        byte lookupOffset = (byte) (idx & 0x1F);
        assert lookupIndex >= 0 && lookupIndex < PAGE_LOOKUP_SIZE;
        int lookupValue = UnsafeHelper.UNSAFE.getIntVolatile(null, pageLookupAddress + lookupIndex);
        if (!Bits.isBitSet(lookupValue, lookupOffset))  {
            // If related bit is not set, this means that the given address cannot be a page address.
            return false;
        } else {
            // Although related bit is set, this doesn't mean that the given address is a page address.
            // So we must check it from page allocations.
            return pageAllocations.containsKey(address);
        }
    }

    private void markPageLookup(long pageAddress) {
        if (pageLookupAddress == NULL_ADDRESS) {
            return;
        }
        // Get related bits and skip LSB 3 bits because every address is 8 bytes aligned.
        int idx = (int) (pageAddress & PAGE_LOOKUP_MASK) >> 3;
        // Find the 4 bytes block contains related lookup bit of given address.
        int lookupIndex = (idx >> 3) & 0xFFFFFFFC;
        // Find the bit offset in the found 4 bytes block.
        byte lookupOffset = (byte) (idx & 0x1F);
        for (;;) {
            int currentLookupValue = UnsafeHelper.UNSAFE.getIntVolatile(null, pageLookupAddress + lookupIndex);
            int newLookupValue = Bits.setBit(currentLookupValue, lookupOffset);
            if (UnsafeHelper.UNSAFE.compareAndSwapInt(null, pageLookupAddress + lookupIndex,
                                                      currentLookupValue, newLookupValue)) {
                break;
            }
        }
    }

    @Override
    protected AddressQueue createAddressQueue(int index, int memorySize) {
        return new GlobalAddressQueue(index, memorySize);
    }

    @Override
    protected int getHeaderSize() {
        return HEADER_SIZE;
    }

    @Override
    protected void onMallocPage(long pageAddress) {
        assertNotNullPtr(pageAddress);
        boolean added = pageAllocations.put(pageAddress, Boolean.TRUE) == null;
        if (added) {
            markPageLookup(pageAddress);
        }
        assert added : "Duplicate malloc() for page address: " + pageAddress;
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
        assert QuickMath.isPowerOfTwo(size) : "Invalid size -> " + size + " is not power of two";
        assert size >= minBlockSize : "Invalid size -> "
                + size + " cannot be smaller than minimum block size " + minBlockSize;
        assert offset >= 0 : "Invalid offset -> " + offset + " is negative";

        int header = initHeader(size);
        long headerAddress = getHeaderAddressByOffset(address, offset);
        if (!UnsafeHelper.UNSAFE.compareAndSwapInt(null, headerAddress, 0, header)) {
            throw new IllegalArgumentException("Wrong size, cannot initialize! Address: " + address
                    + ", Size: " + size + ", Header: " + getSizeFromAddress(address));
        }
        UnsafeHelper.UNSAFE.putIntVolatile(null, address, offset);
    }

    @Override
    protected long allocateExternal(long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long address = pageAllocator.allocate(allocationSize);

        int x = Bits.extractInt(allocationSize, false);
        int y = Bits.extractInt(allocationSize, true);

        long externalHeaderAddress = address;
        UnsafeHelper.UNSAFE.putIntVolatile(null, externalHeaderAddress, y);

        int header = Bits.setBit(x, EXTERNAL_BLOCK_BIT);
        long internalHeaderAddress = address + HEADER_SIZE;
        UnsafeHelper.UNSAFE.putIntVolatile(null, internalHeaderAddress, header);

        return address + EXTERNAL_BLOCK_HEADER_SIZE;
    }

    @Override
    protected void freeExternal(long address, long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long allocationAddress = address - EXTERNAL_BLOCK_HEADER_SIZE;

        if (ASSERTION_ENABLED) {
            int header = getHeader(address);
            if (!Bits.isBitSet(header, EXTERNAL_BLOCK_BIT)) {
                throw new AssertionError("Address " + address + " is not an allocated external address!");
            }
            long actualSize = findSizeExternal(address, header);
            if (actualSize != allocationSize) {
                throw new AssertionError("Invalid size -> actual: " + actualSize
                        + ", expected: " + allocationSize + " (usable size + EXTERNAL_BLOCK_HEADER_SIZE="
                        + EXTERNAL_BLOCK_HEADER_SIZE + ") while free address " + address);
            }
        }

        pageAllocator.free(allocationAddress, allocationSize);
    }

    @Override
    protected void markAvailable(long address) {
        assertNotNullPtr(address);

        long headerAddress = getHeaderAddress(address);
        int header = UnsafeHelper.UNSAFE.getIntVolatile(null, headerAddress);
        assert !isHeaderAvailable(header) : "Address " + address + " has been already marked as available!";

        long pageBase = getOwningPage(address, header);
        if (pageBase == NULL_ADDRESS) {
            throw new IllegalArgumentException("Address " + address + " does not belong to this memory pool!");
        }
        int size = getSizeFromHeader(header);
        assert pageBase + pageSize >= address + size
                : String.format("Block [%,d-%,d] partially overlaps page [%,d-%,d]",
                                address, address + size - 1,
                                pageBase, pageBase + pageSize - 1);
        int pageOffset = (int) (address - pageBase);
        assert pageOffset >= 0 : "Invalid offset -> " + pageOffset + " is negative!";

        int availableHeader = makeHeaderAvailable(header);
        availableHeader = Bits.clearBit(availableHeader, PAGE_OFFSET_EXIST_BIT);

        UnsafeHelper.UNSAFE.putIntVolatile(null, headerAddress, availableHeader);
        UnsafeHelper.UNSAFE.putIntVolatile(null, getPageOffsetAddressBySize(address, size), 0);
        UnsafeHelper.UNSAFE.putIntVolatile(null, address, pageOffset);
    }

    @Override
    protected boolean markUnavailable(long address, int usedSize, int internalSize) {
        assertNotNullPtr(address);

        long headerAddress = getHeaderAddress(address);
        int header = UnsafeHelper.UNSAFE.getIntVolatile(null, headerAddress);
        // This memory address may be merged up (after acquired but not marked as unavailable yet)
        // as buddy by our GarbageCollector thread so its size may be changed.
        // In this case, it must be discarded since it is not served by its current address queue.
        if (getSizeFromHeader(header) != internalSize) {
            return false;
        }

        int availableHeader = makeHeaderAvailable(header);
        int unavailableHeader = makeHeaderUnavailable(header);
        boolean pageOffsetExist = false;
        if (internalSize - usedSize >= MEMORY_OVERHEAD_WHEN_PAGE_OFFSET_IS_STORED) {
            // If there is enough space for storing page offset,
            // set the header as page offset is stored in the memory block.
            unavailableHeader = Bits.setBit(unavailableHeader, PAGE_OFFSET_EXIST_BIT);
            pageOffsetExist = true;
        } else {
            unavailableHeader = Bits.clearBit(unavailableHeader, PAGE_OFFSET_EXIST_BIT);
        }
        if (UnsafeHelper.UNSAFE.compareAndSwapInt(null, headerAddress, availableHeader, unavailableHeader)) {
            int offset = getOffset(address);
            if (pageOffsetExist) {
                // If page offset will be stored, write it to the unused part of the memory block.
                UnsafeHelper.UNSAFE.putIntVolatile(null, getPageOffsetAddressBySize(address, internalSize), offset);
            }
            UnsafeHelper.UNSAFE.putIntVolatile(null, address, 0);
            return true;
        }
        return false;
    }

    @Override
    protected boolean isAvailable(long address) {
        return isAddressAvailable(address);
    }

    @Override
    protected boolean markInvalid(long address, int expectedSize, int offset) {
        assertNotNullPtr(address);

        long headerAddress = getHeaderAddress(address);
        int expectedHeader = initHeader(expectedSize);
        if (UnsafeHelper.UNSAFE.compareAndSwapInt(null, headerAddress, expectedHeader, 0)) {
            UnsafeHelper.UNSAFE.putIntVolatile(null, getPageOffsetAddressBySize(address, expectedSize), 0);
            UnsafeHelper.UNSAFE.putIntVolatile(null, address, 0);
            return true;
        }
        return false;
    }

    @Override
    protected boolean isValidAndAvailable(long address, int expectedSize) {
        assertNotNullPtr(address);

        int header = getHeader(address);

        boolean available = isHeaderAvailable(header);
        if (!available) {
            return false;
        }

        int size = getSizeFromHeader(header);
        if (size != expectedSize) {
            return false;
        }

        int offset = getOffset(address);
        if (offset < 0 || QuickMath.modPowerOfTwo(offset, size) != 0) {
            return false;
        }

        return lookupPage(address - offset);
    }

    private long findSizeExternal(long address, int header) {
        int x = Bits.clearBit(header, EXTERNAL_BLOCK_BIT);
        int y = UnsafeHelper.UNSAFE.getIntVolatile(null, address - EXTERNAL_BLOCK_HEADER_SIZE);
        return Bits.combineToLong(x, y);
    }

    private long findSize(long address, int header) {
        if (Bits.isBitSet(header, EXTERNAL_BLOCK_BIT)) {
            return findSizeExternal(address, header);
        } else {
            return getSizeFromHeader(header);
        }
    }

    @Override
    protected long getSizeInternal(long address) {
        int header = getHeader(address);
        return findSize(address, header);
    }

    @Override
    public long validateAndGetAllocatedSize(long address) {
        assertNotNullPtr(address);

        int header = getHeader(address);
        long size = findSize(address, header);

        if (size > pageSize) {
            return size;
        } else {
            if (isHeaderAvailable(header) || !QuickMath.isPowerOfTwo(size) || size < minBlockSize ) {
                return SIZE_INVALID;
            }
            long page = getOwningPage(address, header);
            return page != NULL_ADDRESS && page + pageSize >= address + size ? size : SIZE_INVALID;
        }
    }

    @Override
    protected int getOffset(long address) {
        return UnsafeHelper.UNSAFE.getIntVolatile(null, address);
    }

    private long getOwningPage(long address, int header) {
        if (Bits.isBitSet(header, PAGE_OFFSET_EXIST_BIT)) {
            // If page offset is stored in the memory block, get the offset directly from there
            // and calculate page address by using this offset.

            int pageOffset = UnsafeHelper.UNSAFE.getIntVolatile(null, getPageOffsetAddressByHeader(address, header));
            if (pageOffset < 0 || pageOffset > (pageSize - minBlockSize)) {
                throw new IllegalArgumentException("Invalid page offset for address " + address + ": " + pageOffset
                        + ". Because page offset cannot be `< 0` or `> (pageSize - minBlockSize)`"
                        + " where pageSize is " + pageSize + " and minBlockSize is " + minBlockSize);
            }
            return address - pageOffset;
        } else {
            final Long pageBase = pageAllocations.floorKey(address);
            return pageBase != null && pageBase + pageSize - 1 >= address ? pageBase : NULL_ADDRESS;
        }
    }

    @Override
    protected boolean destroyInternal() {
        if (!destroyed.compareAndSet(false, true)) {
            return false;
        }
        for (int i = 0; i < addressQueues.length; i++) {
            AddressQueue q = addressQueues[i];
            if (q != null) {
                q.destroy();
                addressQueues[i] = null;
            }
        }
        if (!pageAllocations.isEmpty()) {
            for (Long address : pageAllocations.keySet()) {
                freePage(address);
            }
            pageAllocations.clear();
        }
        return true;
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

    @Override
    protected Counter newCounter() {
        return MwCounter.newMwCounter();
    }

    @SuppressFBWarnings({"BC_IMPOSSIBLE_CAST", "BC_IMPOSSIBLE_INSTANCEOF"})
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

        @Override
        public final int getMemorySize() {
            return memorySize;
        }

        @Override
        public int capacity() {
            return queue.capacity();
        }

        @Override
        public final int remaining() {
            return queue.size();
        }

        @Override
        public void destroy() {
            if (queue instanceof GarbageCollectable) {
                gc.deregisterGarbageCollectable((GarbageCollectable) queue);
            }
            gc.deregisterGarbageCollectable(this);
            queue.dispose();
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
