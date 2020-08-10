package com.hazelcast.internal.memory.impl;

/**
 * The subset of the kinds the Memkind heap manager supports. The kinds
 * in this enum are translated in the JNI wrapper layer to the respective
 * Memkind kinds.
 *
 * @see MemkindHeap#createHeap(MemkindKind, long)
 */
public enum MemkindKind {
    /**
     * Regular DRAM kind using standard page size. Can be used to create
     * heap with Memkind on DRAM.
     */
    DRAM,

    /**
     * DRAM kind using huge pages. Requires OS configuration to work.
     */
    DRAM_HUGEPAGES,

    /**
     * Persistent memory kind that can be used if the persistent memory
     * is configured in system memory mode.
     */
    PMEM_DAX_KMEM;
}
