package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.hazelcast.internal.memory.impl.NUMA.NUMA_NODE_UNBOUNDED;
import static com.hazelcast.internal.util.JVMUtil.is32bitJVM;
import static com.hazelcast.internal.util.OsHelper.OS;
import static com.hazelcast.internal.util.OsHelper.isLinux;

/**
 * {@link LibMalloc} implementation for persistent memory backed allocations
 * for volatile use. Makes JNI calls to the Memkind library for all
 * allocation requests.
 * <p/>
 * This implementation can work on top of multiple PMEM heaps with unifying
 * them into a single heap for the memory manager. For this unification
 * one of the two supported allocation strategies is chosen for every
 * allocator thread without further configuration.
 * - Round-robin strategy: The heaps are taken in a global round-robin
 * fashion. Global here means a global sequence is updated with every
 * allocation request that hits this implementation. By making the sequence
 * global, the allocations can be balanced better.
 * - NUMA-aware strategy: This strategy chooses the heap that is NUMA-local
 * to the allocator thread. NUMA locality is determined based on the
 * {@link PersistentMemoryDirectoryConfig#getNumaNode()} configuration
 * and checking if the allocator thread is restricted to run on the CPUs
 * of a single NUMA node (NUMA bound). If either the NUMA node is not
 * specified in the persistent memory configuration, the allocator thread
 * is not NUMA bound or there is any problem with performing the NUMA
 * layout lookup, this strategy is not taken and the round-robin strategy
 * is used instead for the allocator thread.
 * <p/>
 * The allocation strategy chosen for every allocator thread is cached
 * statically in a {@link ThreadLocal}, which means changing NUMA binding
 * during runtime is not respected by this allocator.
 * <p/>
 * If the allocation request cannot be served from the heap chosen by the
 * applied allocation strategy, the allocation request overflows to other
 * heaps with the round-robin strategy to utilize the full capacity even
 * if the allocation requests hit the NUMA nodes' persistent memories in
 * an unbalanced fashion.
 * <p/>
 * Since we don't track the originating heap for the allocated memory
 * blocks, we can't specify the heap internally on the underlying native
 * Memkind calls backing the {@link #realloc(long, long)} and {@link #free(long)}
 * methods. Since Memkind can determine the heap behind a memory address,
 * we don't sacrifice any functionality here.
 * <p/>
 * Creates and locks exclusively the configured persistent memory {@link #directories}.
 */
final class MemkindPmemMalloc implements LibMalloc {
    private static final ILogger LOGGER = Logger.getLogger(MemkindPmemMalloc.class);

    private final ThreadLocal<AllocationStrategy> cachedAllocationStrategy = new ThreadLocal<>();
    private final List<PersistentMemoryDirectory> directories;
    private final ArrayList<MemkindHeap> heaps;
    private final AtomicLong heapSeq = new AtomicLong();
    private final AllocationStrategy roundRobinAllocationStrategy;
    private final boolean numaEnabled;

    private MemkindPmemMalloc(ArrayList<MemkindHeap> heaps, List<PersistentMemoryDirectory> directories) {
        Preconditions.checkTrue(!heaps.isEmpty(), "Heaps must not be empty");
        this.heaps = heaps;
        this.directories = directories;
        this.roundRobinAllocationStrategy = () -> heaps.get(nextHeapIndex());

        // checking if NUMA-aware allocation should be enabled
        boolean numaConfigured = true;
        int maxNumaNodeConfigured = -1;
        for (MemkindHeap heap : heaps) {
            int numaNode = heap.getNumaNode();
            numaConfigured &= numaNode >= 0;
            if (numaNode > maxNumaNodeConfigured) {
                maxNumaNodeConfigured = numaNode;
            }
        }

        this.numaEnabled = numaConfigured && isNumaEnabled(maxNumaNodeConfigured);
    }

    private int nextHeapIndex() {
        return (int) (heapSeq.getAndIncrement() % heaps.size());
    }

    private int currentHeapIndex() {
        return (int) (heapSeq.get() % heaps.size());
    }

    private boolean isNumaEnabled(int maxNumaNodeConfigured) {
        boolean numaAvailable = NUMA.available();
        int maxNumaNode = numaAvailable ? NUMA.maxNumaNode() : 0;
        final boolean numaEnabled;

        if (!numaAvailable) {
            LOGGER.warning("NUMA nodes are configured for the persistent memory directories but the NUMA functions are not "
                    + "available. NUMA awareness is disabled.");
            numaEnabled = false;
        } else if (maxNumaNodeConfigured > maxNumaNode) {
            LOGGER.warning(String.format("Invalid NUMA configuration: greatest available NUMA node in the system is '%d', "
                    + "greatest NUMA node specified in the persistent memory configuration: '%d'. NUMA awareness is "
                    + "disabled.", maxNumaNode, maxNumaNodeConfigured));
            numaEnabled = false;
        } else {
            LOGGER.info("Allocation strategy preferring NUMA-local allocations is enabled for the persistent memory backed "
                    + "allocations");
            numaEnabled = true;
        }
        return numaEnabled;
    }

    static MemkindPmemMalloc create(NativeMemoryConfig config, long size) {
        checkPlatform();
        Preconditions.checkTrue(!config.getPersistentMemoryConfig().getDirectoryConfigs().isEmpty(), "At least one persistent "
                + "memory directory needs to be configured to use the persistent memory allocator.");

        List<PersistentMemoryDirectory> pmemDirectories = config.getPersistentMemoryConfig().getDirectoryConfigs().stream()
                                                                .map(PersistentMemoryDirectory::new)
                                                                .collect(Collectors.toList());
        logPmemDirectories(pmemDirectories);

        ArrayList<MemkindHeap> heaps = new ArrayList<>(pmemDirectories.size());
        long singleHeapSize = size / pmemDirectories.size();
        try {
            for (PersistentMemoryDirectory directory : pmemDirectories) {
                String pmemFilePath = directory.getPersistentMemoryFile().getAbsolutePath();
                heaps.add(MemkindHeap.createPmemHeap(pmemFilePath, singleHeapSize, directory.getNumaNodeId()));
            }
            return new MemkindPmemMalloc(heaps, pmemDirectories);
        } catch (Exception ex) {
            cleanUp(heaps, pmemDirectories);

            throw ex;
        }
    }

    private static void logPmemDirectories(List<PersistentMemoryDirectory> pmemDirectories) {
        StringBuilder sb = new StringBuilder("Using Memkind PMEM memory allocator with paths:\n");
        pmemDirectories.forEach(pmemDir -> sb
                .append("\t- ").append(pmemDir.getPersistentMemoryFile().getAbsolutePath())
                .append(", configured NUMA node: ").append(pmemDir.getNumaNodeId()).append("\n"));
        LOGGER.info(sb.toString());
    }

    static void checkPlatform() {
        if (!isLinux()) {
            throw new UnsupportedOperationException("Persistent memory is not supported on this platform: " + OS
                    + ". Only Linux platform is supported.");
        }

        if (is32bitJVM()) {
            throw new UnsupportedOperationException("Persistent memory is not supported on 32 bit JVM");
        }
    }

    @Override
    public String toString() {
        return "MemkindPmemMalloc";
    }

    @Override
    public long malloc(long size) {
        MemkindHeap heap = takeHeap();
        try {
            long address = heap.allocate(size);
            if (address != NULL_ADDRESS) {
                return address;
            }
        } catch (OutOfMemoryError e) {
            // couldn't allocate from the chosen heap, we iterate over all other heaps to serve the request
        }

        long address = NULL_ADDRESS;
        int startHeapIndex = currentHeapIndex();
        for (int i = 0; i < heaps.size() && address == NULL_ADDRESS; i++) {
            int heapIndex = (startHeapIndex + i) % heaps.size();
            MemkindHeap overflowHeap = heaps.get(heapIndex);
            if (overflowHeap != heap) {
                try {
                    address = overflowHeap.allocate(size);
                } catch (OutOfMemoryError e) {
                    // couldn't allocate from the chosen heap, we iterate over all other heaps to serve the request
                }
            }
        }

        return address;
    }

    @Override
    public long realloc(long address, long size) {
        try {
            return aHeap().realloc(address, size);
        } catch (OutOfMemoryError e) {
            // If the reallocation request was unsuccessful, we don't try
            // to reallocate in other heaps. This is because doing so would
            // break the contract of realloc, since realloc's contract
            // specifies that the data beyond the original memory address
            // is available even if the call returns a different memory
            // address. In that case, it means an alloc->copy->free call
            // chain has to be done by the malloc implementation. We don't
            // know the size of the original memory block behind the
            // "address" here, so we can't copy the data between the original
            // and the new block. The only safe option then is to fail the
            // reallocation request with returning NULL_ADDRESS.
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address) {
        aHeap().free(address);
    }

    @Override
    public void dispose() {
        cleanUp(heaps, directories);
    }

    private static void cleanUp(List<MemkindHeap> heaps, List<PersistentMemoryDirectory> directories) {
        boolean closedHeaps = closeHeaps(heaps);
        boolean disposeDirectories = disposeDirectories(directories);
        if (!closedHeaps || !disposeDirectories) {
            LOGGER.warning("Could not properly clean up the used file system resources.");
        }
    }

    private static boolean disposeDirectories(List<PersistentMemoryDirectory> directories) {
        boolean disposedDirectories = true;
        for (PersistentMemoryDirectory directory : directories) {
            try {
                directory.dispose();
            } catch (Exception ex) {
                LOGGER.severe("Could not dispose PMEM directory " + directory, ex);
                disposedDirectories = false;
            }
        }
        return disposedDirectories;
    }

    private static boolean closeHeaps(List<MemkindHeap> heaps) {
        boolean closedHeaps = true;
        for (MemkindHeap heap : heaps) {
            try {
                heap.close();
            } catch (Exception ex) {
                LOGGER.severe("Could not close heap " + heap, ex);
                closedHeaps = false;
                // just log here, we will throw the exception dropped us into the enclosing catch block
            }
        }
        return closedHeaps;
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    private MemkindHeap takeHeap() {
        // after the first call to this method on the current thread, we have a cached allocation strategy
        AllocationStrategy allocationStrategy = cachedAllocationStrategy.get();
        if (allocationStrategy != null) {
            return allocationStrategy.takeHeap();
        }

        // we check if the current thread should be statically allocated to a NUMA node's PMEM
        if (numaEnabled) {
            int boundedNumaNode = NUMA.currentThreadBoundedNumaNode();
            if (boundedNumaNode != NUMA_NODE_UNBOUNDED) {
                // the current thread is bounded to a single NUMA node
                for (MemkindHeap heap : heaps) {
                    int heapNumaNode = heap.getNumaNode();
                    if (boundedNumaNode == heapNumaNode) {
                        if (LOGGER.isFineEnabled()) {
                            String currentThread = Thread.currentThread().getName();
                            LOGGER.fine(String.format("Cached heap '%s' assigned to NUMA node '%d' for thread '%s'",
                                    heap.getName(), heap.getNumaNode(), currentThread));
                        }
                        cachedAllocationStrategy.set(() -> heap);
                        return heap;
                    }
                }
            }
        }

        // We cache that this thread cannot be assigned statically to a NUMA-local heap.
        // This can be because any of the following:
        // - NUMA layout initialization was not successful and the NUMA layout is not known
        // - the current thread is not bound to a single NUMA node
        // - there is no NUMA node set for the PMEM directories in the configuration
        cachedAllocationStrategy.set(roundRobinAllocationStrategy);
        if (LOGGER.isFineEnabled()) {
            String threadName = Thread.currentThread().getName();
            String message = String.format("Using round-robin heap allocation strategy for thread %s", threadName);
            LOGGER.fine(message);
        }
        return roundRobinAllocationStrategy.takeHeap();
    }

    public MemkindHeap aHeap() {
        // We don't track back the addresses to heaps, Memkind can do it
        // as part of the free/realloc etc calls. It doesn't matter which
        // heap we choose here, Memkind takes a common path for determining
        // the originating heap for the subsequent free/realloc call.
        return heaps.get(0);
    }

    @FunctionalInterface
    private interface AllocationStrategy {
        MemkindHeap takeHeap();
    }

}
