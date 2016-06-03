package com.hazelcast.memory;

import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.util.QuickMath;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.util.QuickMath.log2;

@SuppressWarnings("checkstyle:methodcount")
abstract class AbstractPoolingMemoryManager implements HazelcastMemoryManager, MemoryAllocator {

    // Size of the memory block header for external allocation when allocation size is bigger than page size
    protected static final int EXTERNAL_BLOCK_HEADER_SIZE = 8;

    static final boolean ASSERTION_ENABLED = AbstractPoolingMemoryManager.class.desiredAssertionStatus();

    private static final int STRING_BUILDER_DEFAULT_CAPACITY = 1024;

    /**
     * Power of two block sizes, using buddy memory allocation;
     *
     * 16, 32, 64, 128, 256, 512, 1024, 2k, .... 32k ... 256k ... 1M
     *
     * - All blocks are at least 8-byte aligned
     * - If cache line is 64 bytes; except these sizes (16, 32), all blocks are cache line aligned.
     * - If cache line is 128 bytes; except these sizes (16, 32, 64), all blocks are cache line aligned.
     * - Block sizes lower than cache line size can cause un-aligned cache line access (access that spans 2 cache lines)
     * Memory access that spans 2 cache lines has very bad performance characteristics.
     * We have a trade-off here, between better memory usage vs performance...
     *
     * - See following blog series for more info about aligned/unaligned memory access:
     * http://psy-lob-saw.blogspot.com.tr/2013/01/direct-memory-alignment-in-java.html
     * http://psy-lob-saw.blogspot.com.tr/2013/07/atomicity-of-unaligned-memory-access-in.html
     * http://psy-lob-saw.blogspot.com.tr/2013/09/diving-deeper-into-cache-coherency.html
     */

    // page allocator, to allocate MAX_SIZE memory block from system
    protected final MemoryAllocator pageAllocator;

    final int minBlockSize;
    final int pageSize;
    final int minBlockSizePower;
    final AddressQueue[] addressQueues;
    final PooledNativeMemoryStats memoryStats;

    // system memory allocator
    // system allocations are not count in quota
    // but total system allocations cannot exceed a predefined portion of max off-heap memory
    final SystemMemoryAllocator systemAllocator;

    private final Counter sequenceGenerator;

    AbstractPoolingMemoryManager(int minBlockSize, int pageSize,
                                 LibMalloc malloc, PooledNativeMemoryStats stats) {

        PoolingMemoryManager.checkBlockAndPageSize(minBlockSize, pageSize);

        memoryStats = stats;
        this.minBlockSize = minBlockSize;
        this.pageSize = pageSize;
        this.minBlockSizePower = QuickMath.log2(minBlockSize);

        int length = QuickMath.log2(pageSize) - minBlockSizePower + 1;
        addressQueues = new AddressQueue[length];
        pageAllocator = new StandardMemoryManager(malloc, stats);
        systemAllocator = new SystemMemoryAllocator(malloc);
        sequenceGenerator = newCounter();
    }

    @Override
    public final long allocate(long size) {
        AddressQueue queue = getAddressQueue(size);
        long address;
        if (queue != null) {
            int memorySize = queue.getMemorySize();
            do {
                address = acquireInternal(queue);
                assertValidAddress(address);
            } while (!markUnavailable(address, (int) size, memorySize));

            assert !isAvailable(address);
            memoryStats.addInternalFragmentation(memorySize - size);
            size = memorySize;
        } else {
            address = allocateExternalBlock(size);
            memoryStats.addInternalFragmentation(EXTERNAL_BLOCK_HEADER_SIZE);
            size += EXTERNAL_BLOCK_HEADER_SIZE;
        }
        memoryStats.addUsedNativeMemory(size);
        return address;
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        long newAddress = allocate(newSize);

        long size = Math.min(currentSize, newSize);
        AMEM.copyMemory(address, newAddress, size);

        if (newSize > currentSize) {
            long startAddress = newAddress + currentSize;
            AMEM.setMemory(startAddress, (newSize - currentSize), (byte) 0);
        }
        free(address, currentSize);
        return newAddress;
    }

    @Override
    public final void free(long address, long size) {
        assertValidAddress(address);
        final AddressQueue queue = getAddressQueue(size);
        if (queue != null) {
            int memorySize = queue.getMemorySize();
            zeroOut(address, size);
            if (isAvailable(address)) {
                throw new AssertionError("Double free() -> address: " + address + ", size: " + size);
            }

            assert memorySize == getSizeInternal(address)
                    : "Size mismatch -> header: " + getSizeInternal(address) + ", param: " + memorySize;

            memoryStats.removeInternalFragmentation(memorySize - size);
            markAvailable(address);
            releaseInternal(queue, address);
            size = memorySize;
        } else {
            freeExternalBlock(address, size);
            memoryStats.removeInternalFragmentation(EXTERNAL_BLOCK_HEADER_SIZE);
            size += EXTERNAL_BLOCK_HEADER_SIZE;
        }
        memoryStats.removeUsedNativeMemory(size);
    }

    @Override
    public final MemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public final MemoryAllocator getSystemAllocator() {
        return systemAllocator;
    }

    @Override
    public final void compact() {
        for (AddressQueue queue : addressQueues) {
            compact(queue);
        }
    }

    @Override
    public final long getAllocatedSize(long address) {
        if (ASSERTION_ENABLED) {
            return validateAndGetAllocatedSize(address);
        }
        return getSizeInternal(address);
    }

    @Override
    public final long getUsableSize(long address) {
        if (ASSERTION_ENABLED) {
            return validateAndGetUsableSize(address);
        }
        long allocatedSize = getAllocatedSize(address);
        if (allocatedSize == SIZE_INVALID) {
            return SIZE_INVALID;
        }
        if (allocatedSize > pageSize) {
            return allocatedSize - EXTERNAL_BLOCK_HEADER_SIZE;
        } else {
            return allocatedSize - headerSize();
        }
    }

    @Override
    public final long validateAndGetUsableSize(long address) {
        long allocatedSize = validateAndGetAllocatedSize(address);
        return allocatedSize == SIZE_INVALID ? SIZE_INVALID
             : allocatedSize <= pageSize ? allocatedSize - headerSize()
             : allocatedSize - EXTERNAL_BLOCK_HEADER_SIZE;
    }

    @Override
    public final long newSequence() {
        return sequenceGenerator.inc();
    }

    protected final AddressQueue getAddressQueue(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive: " + size);
        }
        size += headerSize();
        if (size > pageSize) {
            return null;
        }
        int size32 = Math.max((int) size, minBlockSize);
        int powerOfTwoSize = QuickMath.nextPowerOfTwo(size32);
        int ix = log2(powerOfTwoSize) - minBlockSizePower;
        return addressQueues[ix];
    }


    protected final long acquireInternal(AddressQueue queue) {
        long address;
        int memorySize = queue.getMemorySize();
        while ((address = queue.acquire()) != NULL_ADDRESS) {
            if (isValidAndAvailable(address, memorySize)) {
                break;
            }
        }
        if (address == NULL_ADDRESS) {
            try {
                address = splitFromNextQueue(queue);
            } catch (NativeOutOfMemoryError e) {
                onOome(e);
                throw e;
            }
        }
        return address;
    }

    protected final long toHeaderAddress(long blockBase, int pageOffset) {
        // Header of the block at zero offset is at the end of the page; otherwise it is just before the block
        return pageOffset == 0 ? blockBase + pageSize - headerSize() : blockBase - headerSize();
    }

    protected final void compact(AddressQueue queue) {
        int remaining = queue.remaining();
        if (remaining == 0) {
            return;
        }

        if (!queue.beforeCompaction()) {
            return;
        }

        try {
            for (int i = 0; i < remaining; i++) {
                long address = queue.acquire();
                if (address == NULL_ADDRESS) {
                    break;
                }
                if (!isValidAndAvailable(address, queue.getMemorySize())) {
                    continue;
                }
                if (!tryMergeBuddies(queue, address)) {
                    queue.release(address);
                }
            }
        } finally {
            queue.afterCompaction();
        }
    }

    protected final void freePage(long pageAddress) {
        pageAllocator.free(pageAddress, pageSize);
    }

    protected abstract void initialize(long address, int size, int offset);

    /**
     * Allocates a memory block directly from the page allocator. Used for blocks larger than page size.
     *
     * @param size requested size of the block. The block actually allocated will be large enough to also
     *             accomodate the external block header.
     * @return address of the block of the requested size. It is preceded by the external block header.
     */
    protected abstract long allocateExternalBlock(long size);

    /**
     * Frees an external block, which was allocated directly from the page allocator.
     *
     * @param address address of the block. It is directly preceded by the external block header,
     *                which is an integral part of the block actually allocated.
     * @param size    size of the block (excludes header size)
     */
    protected abstract void freeExternalBlock(long address, long size);

    protected abstract AddressQueue createAddressQueue(int index, int memorySize);

    protected abstract int headerSize();

    protected abstract Counter newCounter();

    protected abstract int getQueueMergeThreshold(AddressQueue queue);

    protected abstract void onOome(NativeOutOfMemoryError e);

    protected abstract void onMallocPage(long address);

    protected abstract void markAvailable(long address);

    protected abstract boolean markUnavailable(long address, int usedSize, int internalSize);

    protected abstract boolean isAvailable(long address);

    protected abstract boolean markInvalid(long address, int expectedSize, int offset);

    protected abstract boolean isValidAndAvailable(long address, int expectedSize);

    protected abstract long getSizeInternal(long address);

    protected abstract int getOffsetWithinPage(long address);

    private long splitFromNextQueue(AddressQueue queue) {
        int memorySize = queue.getMemorySize();
        if (memorySize == pageSize) {
            long address = pageAllocator.allocate(pageSize);
            zeroOut(address, pageSize);
            onMallocPage(address);
            initialize(address, pageSize, 0);
            return address;
        } else {
            AddressQueue nextQ = addressQueues[queue.getIndex() + 1];
            long address;
            int offset;

            do {
                address = acquireInternal(nextQ);
                if (address == NULL_ADDRESS) {
                    throw new NativeOutOfMemoryError("Not enough contiguous memory available,"
                            + " cannot acquire " + MemorySize.toPrettyString(memorySize));
                }

                offset = getOffsetWithinPage(address);
            } while (!markInvalid(address, nextQ.getMemorySize(), offset));

            int offset2 = offset + memorySize;
            long address2 = address + memorySize;

            initialize(address, memorySize, offset);
            initialize(address2, memorySize, offset2);
            queue.release(address2);

            return address;
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean tryMergeBuddies(AddressQueue queue, long address) {
        int memorySize = queue.getMemorySize();
        if (memorySize == pageSize) {
            return false;
        }

        int offset = getOffsetWithinPage(address);
        assert QuickMath.modPowerOfTwo(offset, memorySize) == 0 : "Offset: " + offset + " must be factor of " + memorySize;
        int buddyIndex = offset / memorySize;
        long buddyAddress = buddyIndex % 2 == 0 ? (address + memorySize) : (address - memorySize);

        if (!isValidAndAvailable(buddyAddress, memorySize)) {
            return false;
        }

        if (!markInvalid(address, memorySize, offset)) {
            // happens if memory manager is accessed by multiple threads
            return false;
        }

        // need to read offset before invalidation
        int buddyOffset = getOffsetWithinPage(buddyAddress);
        if (!markInvalid(buddyAddress, memorySize, buddyOffset)) {
            // restore status of other buddy back..
            initialize(address, memorySize, offset);
            return false;
        }

        AddressQueue nextQ = addressQueues[queue.getIndex() + 1];
        if (address < buddyAddress) {
            initialize(address, nextQ.getMemorySize(), offset);
            releaseInternal(nextQ, address);
        } else {
            initialize(buddyAddress, nextQ.getMemorySize(), buddyOffset);
            releaseInternal(nextQ, buddyAddress);
        }

        return true;
    }

    final void initializeAddressQueues() {
        for (int i = 0; i < addressQueues.length; i++) {
            addressQueues[i] = createAddressQueue(i, 1 << (i + minBlockSizePower));
        }
    }

    private void releaseInternal(AddressQueue queue, long address) {
        int remaining = queue.remaining();
        if (remaining >= getQueueMergeThreshold(queue)) {
            if (tryMergeBuddies(queue, address)) {
                return;
            }
        }
        queue.release(address);
    }

    private static void zeroOut(long address, long size) {
        assertValidAddress(address);
        assert size > 0 : "Invalid size: " + size;

        AMEM.setMemory(address, size, (byte) 0);
    }

    static void assertValidAddress(long address) {
        assert address > NULL_ADDRESS : String.format("Illegal memory address %x", address);
    }


    // Diagnostic methods

    public final double getFragmentationRatio(int size) {
        if (size <= 0 || size > pageSize) {
            return 0;
        }
        long free = memoryStats.getFreeNative();
        if (free == 0) {
            return 0;
        }
        size = Math.max(size, minBlockSize);
        int powerOfTwoSize = QuickMath.nextPowerOfTwo(size);
        int ix = log2(powerOfTwoSize) - minBlockSizePower;
        long orderTotal = 0;
        for (int i = ix; i < addressQueues.length; i++) {
            AddressQueue q = addressQueues[i];
            orderTotal += (q.remaining() * (long) q.getMemorySize());
        }
        return (free - orderTotal) / ((double) free);
    }

    public final String dump() {
        StringBuilder sb = new StringBuilder(STRING_BUILDER_DEFAULT_CAPACITY);
        sb.append(memoryStats);

        sb.append(":: PoolingMemoryManager ::").append('\n');
        boolean hasQueue = false;
        for (AddressQueue queue : addressQueues) {
            int remaining = queue.remaining();
            if (remaining > 0) {
                for (int i = 0; i < remaining; i++) {
                    long address = queue.acquire();
                    if (isValidAndAvailable(address, queue.getMemorySize())) {
                        queue.release(address);
                    }
                }
                if (queue.remaining() > 0) {
                    hasQueue = true;
                    sb.append("\tQueue[").append(MemorySize.toPrettyString(queue.getMemorySize()))
                            .append("]: ").append(queue.remaining()).append('\n');
                }
            }
        }
        if (!hasQueue) {
            sb.append(" ALL QUEUES ARE EMPTY!").append('\n');
        }
        return sb.toString();
    }

    // for internal pool and metadata usage
    protected final class SystemMemoryAllocator
            implements MemoryAllocator {

        private final LibMalloc malloc;

        public SystemMemoryAllocator(LibMalloc malloc) {
            this.malloc = malloc;
        }

        @Override
        public long allocate(long size) {
            checkSize(size);
            long address = malloc.malloc(size);
            checkAddress(address, size);
            AMEM.setMemory(address, size, (byte) 0);
            memoryStats.addMetadataUsage(size);
            return address;
        }

        private void checkSize(long size) {
            long limit = memoryStats.getMaxMetadata();
            long usage = memoryStats.getUsedMetadata();
            if (usage + size > limit) {
                throw new NativeOutOfMemoryError("System allocations limit exceeded!"
                        + " Limit: " + MemorySize.toPrettyString(limit)
                        + ", usage: " + MemorySize.toPrettyString(usage)
                        + ", requested: " + MemorySize.toPrettyString(size));
            }
        }

        @Override
        public void free(long address, long size) {
            malloc.free(address);
            memoryStats.removeMetadataUsage(size);
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            long diff = newSize - currentSize;
            if (diff > 0) {
                checkSize(diff);
            }
            long newAddress = malloc.realloc(address, newSize);
            checkAddress(newAddress, newSize);

            if (diff > 0) {
                long startAddress = newAddress + currentSize;
                AMEM.setMemory(startAddress, diff, (byte) 0);
            }

            memoryStats.addMetadataUsage(diff);
            return newAddress;
        }

        private void checkAddress(long address, long size) {
            if (address == NULL_ADDRESS) {
                throw new NativeOutOfMemoryError("Not enough contiguous memory available!"
                        + "Cannot acquire " + MemorySize.toPrettyString(size) + '!');
            }
        }

        @Override
        public void dispose() {
        }
    }
}
