package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import static com.hazelcast.internal.memory.impl.MemkindKind.DRAM;
import static com.hazelcast.internal.memory.impl.MemkindKind.DRAM_HUGEPAGES;
import static com.hazelcast.internal.memory.impl.MemkindKind.PMEM_DAX_KMEM;
import static com.hazelcast.internal.memory.impl.MemkindUtil.useMemkind;
import static com.hazelcast.internal.memory.impl.MemkindUtil.useMemkindDaxKmem;
import static com.hazelcast.internal.memory.impl.MemkindUtil.useMemkindHugePages;

/**
 * {@link LibMalloc} implementation for all supported kind of Memkind
 * except for the PMEM kind. See {@link MemkindKind} for the supported
 * kinds. Makes JNI calls to the Memkind library for all allocation requests.
 *
 * @see MemkindKind
 * @see MemkindPmemMalloc
 */
public final class MemkindMalloc extends AbstractMemkindMalloc {
    private static final ILogger LOGGER = Logger.getLogger(MemkindMalloc.class);

    private MemkindMalloc(MemkindHeap memkindHeap) {
        super(memkindHeap);
    }

    static MemkindMalloc create(NativeMemoryConfig config, long size) {
        assert useMemkind();

        final MemkindKind kind;
        if (useMemkindDaxKmem()) {
            kind = PMEM_DAX_KMEM;
        } else if (useMemkindHugePages()) {
            kind = DRAM_HUGEPAGES;
        } else {
            kind = DRAM;
        }

        LOGGER.info("Using Memkind memory allocator with " + kind.name() + " kind");
        MemkindHeap heap = MemkindHeap.createHeap(kind, size);
        return new MemkindMalloc(heap);
    }
}
