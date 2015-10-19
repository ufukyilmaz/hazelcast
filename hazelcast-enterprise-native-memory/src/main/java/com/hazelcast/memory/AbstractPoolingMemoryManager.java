package com.hazelcast.memory;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.QuickMath;

import static com.hazelcast.util.QuickMath.log2;

/**
 * @author mdogan 03/12/13
 */
abstract class AbstractPoolingMemoryManager implements MemoryManager {

    /**
     * Power of two block sizes, using buddy memory allocation;
     * 
     * 16, 32, 64, 128, 256, 512, 1024, 2k, .... 32k ... 256k ... 1M
     *
     *  - All blocks are at least 8-byte aligned
     *  - If cache line is 64 bytes; except these sizes (16, 32), all blocks are cache line aligned.
     *  - If cache line is 128 bytes; except these sizes (16, 32, 64), all blocks are cache line aligned.
     *  - Block sizes lower than cache line size can cause un-aligned cache line access (access that spans 2 cache lines)
     *    Memory access that spans 2 cache lines has very bad performance characteristics.
     *    We have a trade-off here, between better memory usage vs performance...
     *
     *  - See following blog series for more info about aligned/unaligned memory access:
     *    http://psy-lob-saw.blogspot.com.tr/2013/01/direct-memory-alignment-in-java.html
     *    http://psy-lob-saw.blogspot.com.tr/2013/07/atomicity-of-unaligned-memory-access-in.html
     *    http://psy-lob-saw.blogspot.com.tr/2013/09/diving-deeper-into-cache-coherency.html
     * 
     */

    final int minBlockSize;
    final int pageSize;
    final int minBlockSizePower;
    final AddressQueue[] addressQueues;
    final PooledNativeMemoryStats memoryStats;

    // page allocator, to allocate MAX_SIZE memory block from system
    private final MemoryAllocator pageAllocator;

    // system memory allocator
    // system allocations are not count in quota
    // but total system allocations cannot exceed a predefined portion of max off-heap memory
    final SystemMemoryAllocator systemAllocator;

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
    }

    final void initializeAddressQueues() {
        for (int i = 0; i < addressQueues.length; i++) {
            addressQueues[i] = createAddressQueue(i, 1 << (i + minBlockSizePower));
        }
    }

    protected abstract AddressQueue createAddressQueue(int index, int memorySize);

    protected abstract int getHeaderSize();

    protected final AddressQueue getAddressQueue(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }
        size += getHeaderSize();
        if (size > pageSize) {
            return null;
        }
        int size32 = Math.max((int) size, minBlockSize);
        int powerOfTwoSize = QuickMath.nextPowerOfTwo(size32);
        int ix = log2(powerOfTwoSize) - minBlockSizePower;
        return addressQueues[ix];
    }

    static void assertNotNullPtr(long address) {
        assert address != NULL_ADDRESS : "Illegal memory address: " + address;
    }

    @Override
    public final long allocate(long size) {
        AddressQueue queue = getAddressQueue(size);
        long address;
        if (queue != null) {
            int memorySize = queue.getMemorySize();
            do {
                address = acquireInternal(queue);
                assertNotNullPtr(address);
            } while (!markUnavailable(address, memorySize));

            assert !isAvailable(address);
            address += getHeaderSize();
            memoryStats.addInternalFragmentation(queue.getMemorySize() - size);
            size = queue.getMemorySize();
        } else {
            address = pageAllocator.allocate(size);
        }
        memoryStats.addUsedOffHeap(size);
        return address;
    }

    // TODO: loopify acquireInternal() & splitFromNextQueue() recursion
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

    protected abstract void onOome(NativeOutOfMemoryError e);

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        long newAddress = allocate(newSize);

        long size = Math.min(currentSize, newSize);
        UnsafeHelper.UNSAFE.copyMemory(address, newAddress, size);

        if (newSize > currentSize) {
            long startAddress = newAddress + currentSize;
            UnsafeHelper.UNSAFE.setMemory(startAddress, (newSize - currentSize), (byte) 0);
        }
        free(address, currentSize);
        return newAddress;
    }

    @Override
    public final void free(long address, long size) {
        assertNotNullPtr(address);
        final AddressQueue queue = getAddressQueue(size);
        if (queue != null) {
            zero(address, size);
            address -= getHeaderSize();
            if (isAvailable(address)) {
                throw new AssertionError("Double free(): " + address);
            }

            assert queue.getMemorySize() == getSizeInternal(address)
                    : "Size mismatch -> header: " + getSizeInternal(address) + ", param: " + queue.getMemorySize();

            memoryStats.addInternalFragmentation(size - queue.getMemorySize());
            markAvailable(address);
            releaseInternal(queue, address);
            size = queue.getMemorySize();
        } else {
            pageAllocator.free(address, size);
        }
        memoryStats.addUsedOffHeap(-size);
    }

    private static void zero(long address, long size) {
        assertNotNullPtr(address);
        assert size > 0 : "Invalid size: " + size;

        UnsafeHelper.UNSAFE.setMemory(address, size, (byte) 0);
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

    protected abstract int getQueueMergeThreshold(AddressQueue queue);

    private boolean tryMergeBuddies(AddressQueue queue, long address) {
        final int memorySize = queue.getMemorySize();
        if (memorySize == pageSize) {
            return false;
        }

        int offset = getOffset(address);
        assert QuickMath.modPowerOfTwo(offset, memorySize) == 0
                : "Offset: " + offset + " must be factor of " + memorySize;
        int buddyIndex = offset / memorySize;
        long buddyAddress = buddyIndex % 2 == 0 ? (address + memorySize) : (address - memorySize);

        if (!isValidAndAvailable(buddyAddress, memorySize)) {
            return false;
        }

        if (!markInvalid(address, memorySize)) {
            // happens if memory manager is accessed by multiple threads
            return false;
        }

        // need to read offset before invalidation
        int buddyOffset = getOffset(buddyAddress);
        if (!markInvalid(buddyAddress, memorySize)) {
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

    @Override
    public final void compact() {
        for (AddressQueue queue : addressQueues) {
            compact(queue);
        }
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

    private long splitFromNextQueue(AddressQueue queue) {
        int memorySize = queue.getMemorySize();
        if (memorySize == pageSize) {
            long address = pageAllocator.allocate(pageSize);
            zero(address, pageSize);
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
                    throw new NativeOutOfMemoryError("Not enough contiguous memory available! " +
                            "Cannot acquire " + MemorySize.toPrettyString(memorySize) + "!");
                }

                offset = getOffset(address);
            } while (!markInvalid(address, nextQ.getMemorySize()));

            int offset2 = offset + memorySize;
            long address2 = address + memorySize;

            initialize(address, memorySize, offset);
            initialize(address2, memorySize, offset2);
            queue.release(address2);

            return address;
        }
    }

    protected final void freePage(long pageAddress) {
        pageAllocator.free(pageAddress, pageSize);
    }

    protected abstract void onMallocPage(long address);

    protected abstract void initialize(long address, int size, int offset);

    protected abstract void markAvailable(long address);

    protected abstract boolean markUnavailable(long address, int expectedSize);

    protected abstract boolean isAvailable(long address);

    protected abstract boolean markInvalid(long address, int expectedSize);

    protected abstract boolean isValidAndAvailable(long address, int expectedSize);

    protected abstract int getSizeInternal(long address);

    protected abstract int getOffset(long address);

    public abstract int getHeaderLength();

    @Override
    public final MemoryStats getMemoryStats() {
        return memoryStats;
    }

    public final double getFragmentationRatio(int size) {
        if (size <= 0 || size > pageSize) {
            return 0;
        }
        long free = memoryStats.getFreeNativeMemory();
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
        StringBuilder s = new StringBuilder(1024);
        s.append(memoryStats);

        s.append(":: PoolingMemoryManager ::").append('\n');
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
                    s.append("\tQueue[").append(MemorySize.toPrettyString(queue.getMemorySize()))
                            .append("]: ").append(queue.remaining()).append('\n');
                }
            }
        }
        if (!hasQueue) {
            s.append(" ALL QUEUES ARE EMPTY!").append('\n');
        }
        return s.toString();
    }

    @Override
    public final MemoryAllocator unwrapMemoryAllocator() {
        return systemAllocator;
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
            UnsafeHelper.UNSAFE.setMemory(address, size, (byte) 0);
            memoryStats.addMetadataUsage(size);
            return address;
        }

        private void checkSize(long size) {
            long limit = memoryStats.getMaxMetadata();
            long usage = memoryStats.getUsedMetadata();
            if (usage + size > limit) {
                throw new NativeOutOfMemoryError("System allocations limit exceeded! " +
                        "Limit: " + MemorySize.toPrettyString(limit)
                        + ", usage: " + MemorySize.toPrettyString(usage)
                        + ", requested: " + MemorySize.toPrettyString(size));
            }
        }

        @Override
        public void free(long address, long size) {
            malloc.free(address);
            memoryStats.addMetadataUsage(-size);
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
                UnsafeHelper.UNSAFE.setMemory(startAddress, diff, (byte) 0);
            }

            memoryStats.addMetadataUsage(diff);
            return newAddress;
        }

        private void checkAddress(long address, long size) {
            if (address == NULL_ADDRESS) {
                throw new NativeOutOfMemoryError("Not enough contiguous memory available! " +
                        "Cannot acquire " + MemorySize.toPrettyString(size) + "!");
            }
        }
    }
}
