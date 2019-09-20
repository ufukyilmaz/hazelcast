package com.hazelcast.memory;

import com.hazelcast.elastic.queue.LongLinkedBlockingQueue;
import com.hazelcast.elastic.queue.LongQueue;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.nio.Bits;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.QuickMath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static java.lang.String.format;

/**
 * Singleton global memory manager used for all allocations requests done on a thread which was not
 * registered with the {@link PoolingMemoryManager}. Concurrent allocation requests will contend for access
 * and performance will suffer.
 */
@SuppressWarnings("checkstyle:methodcount")
final class GlobalPoolingMemoryManager extends AbstractPoolingMemoryManager {

    // Size of the memory block header in bytes
    private static final int HEADER_SIZE = 4;
    // Using sign bit as block is allocated externally from page allocator (OS) directly.
    // for allocations bigger than page size.
    private static final int EXTERNAL_BLOCK_BIT = Integer.SIZE - 1;
    // Using sign bit as available bit
    // so the first bit represents that is this memory block is available or not (in use already)
    private static final int AVAILABLE_BIT = EXTERNAL_BLOCK_BIT - 1;
    // We store size as encoded by shifting it 3 bit since it is always multiply of 8.
    private static final int SIZE_SHIFT_COUNT = 3;

    // Initial capacity for various internal allocations such as page addresses, address queues, etc ...
    private static final int INITIAL_CAPACITY = 2048;

    // We consider LSB 24 bits (3 bytes)
    // It is enough to detect non-page addresses.
    @SuppressWarnings("checkstyle:magicnumber")
    private static final int PAGE_LOOKUP_LENGTH = Integer.getInteger("hazelcast.memory.pageLookupLength", 1 << 24);

    // Size of page lookup allocation (256K)
    private static final int PAGE_LOOKUP_SIZE =
            // All addresses are 8 byte aligned, so no need to consider LSB 3 bits
            (PAGE_LOOKUP_LENGTH >> 3)
                    // We use bits as flag in each byte to index, so we handle 8 index flag for each byte.
                    >> 3;

    // Mask to get related bits of address for page lookup.
    // We consider LSB 24 bits (3 bytes). It is enough to detect non-page addresses.
    private static final int PAGE_LOOKUP_MASK = PAGE_LOOKUP_LENGTH - 1;

    /*
     * External Memory Block Header Structure: (4 byte = 32 bits)
     *
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Encoded size           |   28 bits            | ==> Size is encoded by shifting 3 bit
     * +------------------------+----------------------+
     * | <RESERVED>             |   1 bit              |
     * +------------------------+----------------------+
     * | <RESERVED>             |   1 bit              |
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
     * | <RESERVED>             |   31 bits            |
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
     * | <RESERVED>             |   4 bytes (int)      |
     * +------------------------+----------------------+
     * | Header                 |   4 bytes (int)      |
     * +------------------------+----------------------+
     * | Usable Memory Region   |   <size>             |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */

    private final GarbageCollector gc;
    private final ConcurrentNavigableMap<Long, Object> pageAllocations
            = new ConcurrentSkipListMap<Long, Object>();
    private final ConcurrentMap<Long, Long> externalAllocations
            = new ConcurrentHashMap<Long, Long>();
    // We have a lookup to detect non-page addresses.
    // If an address is not flagged in lookup, this means that it is definitely not a page address.
    // But if the flag is set, this doesn't mean that it is a page address.
    // In this case, it must be checked from page allocations.
    private final long pageLookupAddress;

    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    private volatile long lastFullCompaction;

    GlobalPoolingMemoryManager(int minBlockSize, int pageSize, LibMalloc malloc, PooledNativeMemoryStats stats,
                               GarbageCollector gc) {
        super(minBlockSize, pageSize, malloc, stats);
        this.gc = gc;
        this.pageLookupAddress = initPageLookup();
        initializeAddressQueues();
    }

    private long initPageLookup() {
        long pageLookupAddr = NULL_ADDRESS;
        try {
            pageLookupAddr = systemAllocator.allocate(PAGE_LOOKUP_SIZE);
            AMEM.setMemory(pageLookupAddr, PAGE_LOOKUP_SIZE, (byte) 0x00);
        } catch (NativeOutOfMemoryError oome) {
            // TODO: should we log this?
            EmptyStatement.ignore(oome);
        }
        return pageLookupAddr;
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
        if (isPageBaseAddress(address)) {
            // If this is the first block, wrap around
            return address + pageSize - HEADER_SIZE;
        }
        return address - HEADER_SIZE;
    }

    private int getHeader(long address) {
        long headerAddress = getHeaderAddress(address);
        return AMEM.getIntVolatile(null, headerAddress);
    }

    private static boolean isHeaderAvailable(int header) {
        return Bits.isBitSet(header, AVAILABLE_BIT);
    }

    private boolean isAddressAvailable(long address) {
        int header = getHeader(address);
        return isHeaderAvailable(header);
    }

    private static int getSizeFromHeader(int header) {
        int size = Bits.clearBit(header, AVAILABLE_BIT);
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

    @SuppressWarnings("checkstyle:magicnumber")
    private boolean isPageBaseAddress(long address) {
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
        int lookupValue = AMEM.getIntVolatile(null, pageLookupAddress + lookupIndex);
        if (!Bits.isBitSet(lookupValue, lookupOffset)) {
            // If related bit is not set, this means that the given address cannot be a page address.
            return false;
        }
        // Although related bit is set, this doesn't mean that the given address is a page address.
        // So we must check it from page allocations.
        return pageAllocations.containsKey(address);
    }

    @SuppressWarnings("checkstyle:magicnumber")
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
        for (; ; ) {
            int currentLookupValue = AMEM.getIntVolatile(null, pageLookupAddress + lookupIndex);
            int newLookupValue = Bits.setBit(currentLookupValue, lookupOffset);
            if (AMEM.compareAndSwapInt(null, pageLookupAddress + lookupIndex,
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
    protected int headerSize() {
        return HEADER_SIZE;
    }

    @Override
    protected void onMallocPage(long pageAddress) {
        assertValidAddress(pageAddress);
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
        assertValidAddress(address);
        assert QuickMath.isPowerOfTwo(size) : "Invalid size -> " + size + " is not power of two";
        assert size >= minBlockSize : "Invalid size -> "
                + size + " cannot be smaller than minimum block size " + minBlockSize;

        int header = initHeader(size);
        long headerAddress = toHeaderAddress(address, offset);
        if (!AMEM.compareAndSwapInt(null, headerAddress, 0, header)) {
            throw new IllegalArgumentException("Wrong size, cannot initialize! Address: " + address
                    + ", Size: " + size + ", Header: " + getSizeFromAddress(address));
        }
    }

    @Override
    protected long allocateExternalBlock(long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long address = pageAllocator.allocate(allocationSize);

        Long existingAllocationSize = externalAllocations.putIfAbsent(address, allocationSize);
        if (existingAllocationSize != null) {
            pageAllocator.free(address, allocationSize);
            throw new AssertionError("Duplicate malloc() for external address " + address);
        }

        int header = Bits.setBit(0, EXTERNAL_BLOCK_BIT);
        long internalHeaderAddress = address + EXTERNAL_BLOCK_HEADER_SIZE - HEADER_SIZE;
        AMEM.putIntVolatile(null, internalHeaderAddress, header);

        return address + EXTERNAL_BLOCK_HEADER_SIZE;
    }

    @Override
    protected void freeExternalBlock(long address, long size) {
        long allocationSize = size + EXTERNAL_BLOCK_HEADER_SIZE;
        long allocationAddress = address - EXTERNAL_BLOCK_HEADER_SIZE;

        Long actualAllocationSize = externalAllocations.remove(allocationAddress);
        if (actualAllocationSize == null) {
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
        assertValidAddress(address);

        long headerAddress = getHeaderAddress(address);
        int header = AMEM.getIntVolatile(null, headerAddress);
        assert !isHeaderAvailable(header) : "Address " + address + " has been already marked as available!";

        long pageBase = getOwningPage(address);
        if (pageBase == NULL_ADDRESS) {
            throw new IllegalArgumentException("Address " + address + " does not belong to this memory pool!");
        }
        int size = getSizeFromHeader(header);
        assert pageBase + pageSize >= address + size
                : format("Block [%,d-%,d] partially overlaps page [%,d-%,d]",
                address, address + size - 1,
                pageBase, pageBase + pageSize - 1);
        int availableHeader = makeHeaderAvailable(header);

        AMEM.putIntVolatile(null, headerAddress, availableHeader);
    }

    @Override
    protected boolean markUnavailable(long address, int usedSize, int internalSize) {
        assertValidAddress(address);

        long headerAddress = getHeaderAddress(address);
        int header = AMEM.getIntVolatile(null, headerAddress);
        // This memory address may be merged up (after acquired but not marked as unavailable yet)
        // as buddy by our GarbageCollector thread so its size may be changed.
        // In this case, it must be discarded since it is not served by its current address queue.
        if (getSizeFromHeader(header) != internalSize) {
            return false;
        }

        int availableHeader = makeHeaderAvailable(header);
        int unavailableHeader = makeHeaderUnavailable(header);
        return AMEM.compareAndSwapInt(null, headerAddress, availableHeader, unavailableHeader);
    }

    @Override
    protected boolean isAvailable(long address) {
        return isAddressAvailable(address);
    }

    @Override
    protected boolean markInvalid(long address, int expectedSize, int offset) {
        assertValidAddress(address);

        long headerAddress = toHeaderAddress(address, offset);
        int expectedHeader = initHeader(expectedSize);
        return AMEM.compareAndSwapInt(null, headerAddress, expectedHeader, 0);
    }

    @Override
    protected boolean isValidAndAvailable(long address, int expectedSize) {
        assertValidAddress(address);

        int header = getHeader(address);

        boolean available = isHeaderAvailable(header);
        if (!available) {
            return false;
        }

        int size = getSizeFromHeader(header);
        if (size != expectedSize) {
            return false;
        }

        return true;
    }

    private long findSizeExternal(long address) {
        long allocationAddress = address - EXTERNAL_BLOCK_HEADER_SIZE;
        Long allocationSize = externalAllocations.get(allocationAddress);
        if (allocationSize == null) {
            return SIZE_INVALID;
        } else {
            return allocationSize;
        }
    }

    private long findSize(long address, int header) {
        if (Bits.isBitSet(header, EXTERNAL_BLOCK_BIT)) {
            return findSizeExternal(address);
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
        assertValidAddress(address);

        int header = getHeader(address);
        long size = findSize(address, header);

        if (size > pageSize) {
            return size;
        } else {
            if (isHeaderAvailable(header) || !QuickMath.isPowerOfTwo(size) || size < minBlockSize) {
                return SIZE_INVALID;
            }
            long page = getOwningPage(address);
            return page != NULL_ADDRESS && page + pageSize >= address + size ? size : SIZE_INVALID;
        }
    }

    @Override
    protected int getOffsetWithinPage(long address) {
        return (int) (address - getOwningPage(address));
    }

    private long getOwningPage(long address) {
        Long pageBase = pageAllocations.floorKey(address);
        return pageBase != null && pageBase + pageSize - 1 >= address ? pageBase : NULL_ADDRESS;
    }

    @Override
    public void dispose() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < addressQueues.length; i++) {
            AddressQueue q = addressQueues[i];
            if (q != null) {
                q.destroy();
                addressQueues[i] = DestroyedAddressQueue.INSTANCE;
            }
        }
        freePageAllocations();
        freeExternalAllocations();
        if (pageLookupAddress != NULL_ADDRESS) {
            systemAllocator.free(pageLookupAddress, PAGE_LOOKUP_SIZE);
        }
    }

    private void freePageAllocations() {
        if (!pageAllocations.isEmpty()) {
            for (Long address : pageAllocations.keySet()) {
                freePage(address);
            }
            pageAllocations.clear();
        }
    }

    private void freeExternalAllocations() {
        if (!externalAllocations.isEmpty()) {
            Iterator<Map.Entry<Long, Long>> iterator = externalAllocations.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Long> entry = iterator.next();
                long address = entry.getKey();
                long size = entry.getValue();
                pageAllocator.free(address, size);
                iterator.remove();
            }
        }
    }

    @Override
    public boolean isDisposed() {
        // TODO: this memory manager is multi-threaded, so there is no sync relation between destroy() and other methods
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
    private final class GlobalAddressQueue implements AddressQueue, GarbageCollectable {

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
        public long acquire() {
            return queue.poll();
        }

        @Override
        public boolean release(long address) {
            if (address == INVALID_ADDRESS) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            return queue.offer(address);
        }

        @Override
        public int getMemorySize() {
            return memorySize;
        }

        @Override
        public int capacity() {
            return queue.capacity();
        }

        @Override
        public int remaining() {
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
            return "GlobalAddressQueue{" + "memorySize=" + MemorySize.toPrettyString(memorySize) + '}';
        }
    }

    @Override
    public String toString() {
        return "GlobalPoolingMemoryManager";
    }
}
