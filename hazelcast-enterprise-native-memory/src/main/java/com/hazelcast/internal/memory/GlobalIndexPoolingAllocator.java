package com.hazelcast.internal.memory;

import com.hazelcast.internal.elastic.queue.LongLinkedBlockingQueue;
import com.hazelcast.internal.elastic.queue.LongQueue;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.NativeOutOfMemoryError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.memory.AbstractPoolingMemoryManager.assertValidAddress;
import static com.hazelcast.internal.memory.AbstractPoolingMemoryManager.MetadataMemoryAllocator;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.util.QuickMath.isPowerOfTwo;

/**
 * Global index  memory manager (GIMM) specialized for all allocation requests done on behalf of the global
 * concurrent B+Tree index.
 * <p>
 * The GIMM manages memory of fixed blocks. The size of the block is equal to the node size of the B+tree
 * which is used as a global HD index.
 * <p>
 * The GIMM never merges or splits the blocks. The released block still can be accessed by the B+tree operations
 * so that on release, the memory of the block shouldn't be zeroed or modified.
 * <p>
 * The GIMM doesn't support blocks compaction, because by design the blocks are fixed size.
 * <p>
 * Only header fields of the released block can be accessed by B+tree operations,
 * that is a lockState (first 8 bytes of the block) and sequenceNumber fields.
 *
 */
public final class GlobalIndexPoolingAllocator implements MemoryAllocator {

    /**
     * The default B+tree's node size
     */
    public static final int DEFAULT_BTREE_INDEX_NODE_SIZE = 8192;

    // The node size of the B+tree
    private final int nodeSize;

    private final PooledNativeMemoryStats memoryStats;
    private volatile AddressQueue addressQueue;

    private final MemoryAllocator nodeAllocator;
    private final MetadataMemoryAllocator systemAllocator;

    private final AtomicBoolean destroyed = new AtomicBoolean();

    public GlobalIndexPoolingAllocator(LibMalloc malloc, PooledNativeMemoryStats memoryStats) {
        this(malloc, memoryStats, DEFAULT_BTREE_INDEX_NODE_SIZE);
    }

    public GlobalIndexPoolingAllocator(LibMalloc malloc, PooledNativeMemoryStats memoryStats, int nodeSize) {
        this.nodeSize = nodeSize;
        this.memoryStats = memoryStats;

        if (!isPowerOfTwo(nodeSize)) {
            throw new IllegalArgumentException("Node size must be power of two! -> " + nodeSize);
        }

        nodeAllocator = new StandardMemoryManager(malloc, memoryStats);
        systemAllocator = new MetadataMemoryAllocator(malloc, memoryStats);
        addressQueue = new GlobalIndexAddressQueue(nodeSize);
    }

    public int getNodeSize() {
        return nodeSize;
    }

    @Override
    public long allocate(long size) {
        checkSize(size);

        long address = addressQueue.acquire();

        if (address == NULL_ADDRESS) {
            try {
                address = nodeAllocator.allocate(nodeSize);
            } catch (NativeOutOfMemoryError e) {
                onOome(e);
                throw e;
            }
            zeroOutHeader(address);
            onMallocPage(address);
        }

        memoryStats.addUsedNativeMemory(size);

        return address;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static void zeroOutHeader(long address) {
        assertValidAddress(address);
        // Make sure the lock state (8 bytes) and sequence number (next 8 bytes) are 0
        AMEM.setMemory(address, 16, (byte) 0);
    }

    protected void onMallocPage(long pageAddress) {
        assertValidAddress(pageAddress);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        assertValidAddress(address);
        assert currentSize == newSize;
        return address;
    }

    @Override
    public void free(long address, long size) {
        assertValidAddress(address);
        checkSize(size);

        // Don't zero out the node's header, it is still can be accessed by the
        // concurrent iterator
        addressQueue.release(address);
        memoryStats.removeUsedNativeMemory(size);
    }

    private void onOome(NativeOutOfMemoryError e) {
        // No compaction, all blocks have the same size
    }

    @Override
    public void dispose() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }

        long address;
        while ((address = addressQueue.acquire()) != NULL_ADDRESS) {
            nodeAllocator.free(address, nodeSize);
        }
        addressQueue.destroy();
        addressQueue = DestroyedAddressQueue.INSTANCE;
    }

    private void checkSize(long size) {
        if (size != nodeSize) {
            throw new IllegalArgumentException("Size " + size + " must be equal to " + nodeSize);
        }
    }

    // For unit testing only
    public List<Long> consumeNodeAddressesFromQueue() {
        List<Long> nodes = new ArrayList<>();
        long address;
        while ((address = addressQueue.acquire()) != NULL_ADDRESS) {
            nodes.add(address);
        }
        return nodes;
    }

    private final class GlobalIndexAddressQueue implements AddressQueue {

        private final int memorySize;
        private final LongQueue queue;

        private GlobalIndexAddressQueue(int memorySize) {
            this.memorySize = memorySize;
            this.queue = createQueue();
        }

        private LongLinkedBlockingQueue createQueue() {
            return new LongLinkedBlockingQueue(systemAllocator, INVALID_ADDRESS);
        }

        @Override
        public boolean beforeCompaction() {
            // no compaction is needed
            return false;
        }

        @Override
        public void afterCompaction() {
            // no-op
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
            queue.dispose();
        }

        @Override
        public int getIndex() {
            return 0;
        }

        @Override
        public String toString() {
            return "GlobalIndexAddressQueue{" + "memorySize=" + MemorySize.toPrettyString(memorySize) + '}';
        }
    }

    @Override
    public String toString() {
        return "GlobalIndexPoolingAllocator";
    }

}
