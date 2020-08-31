package com.hazelcast.internal.memory.impl;

/**
 * Entry class for NUMA related utility functions such as checking if the
 * current thread is bounded to a NUMA node. This class makes native calls
 * to libnuma.
 * <p/>
 * Note that this class doesn't make an attempt to load libnuma.so, it
 * should be loaded before this class is used. The reason is that this
 * class is meant to be a helper class for the NUMA-aware allocation
 * strategy for {@link MemkindPmemMalloc}, which loads libnuma through
 * {@link MemkindHeap} as libmemkind's direct dependency.
 *
 * @see MemkindPmemMalloc
 */
final class NUMA {

    /**
     * NUMA node id returned by {@linkplain #currentThreadBoundedNumaNode()}
     * if the current thread is not bounded to a single NUMA node.
     */
    static final int NUMA_NODE_UNBOUNDED = -1;

    private NUMA() {
    }

    /**
     * Checks if the NUMA library is available.
     *
     * @return {@code true} on success, {@code false} otherwise.
     */
    static native boolean available();

    /**
     * Retrieves the NUMA node id that the currently running thread is
     * bounded to. If the current thread is not bounded to a single NUMA
     * node, the call returns {@linkplain #NUMA_NODE_UNBOUNDED}.
     * <p/>
     * This call is relatively expensive as it retrieves the current
     * thread's native PID with a syscall, gets the CPU affinity for it
     * and compares the affinity to the NUMA nodes' CPU sets to perform
     * the check.
     *
     * @return the id of the NUMA node or {@linkplain #NUMA_NODE_UNBOUNDED}
     * otherwise
     */
    static native int currentThreadBoundedNumaNode();

    /**
     * Retrieves the greatest NUMA node id.
     *
     * @return the greatest NUMA node id
     */
    static native int maxNumaNode();
}
