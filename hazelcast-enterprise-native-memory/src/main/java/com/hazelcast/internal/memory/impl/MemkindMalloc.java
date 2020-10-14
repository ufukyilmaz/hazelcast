package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import static com.hazelcast.config.PersistentMemoryMode.SYSTEM_MEMORY;
import static com.hazelcast.internal.memory.impl.MemkindUtil.configuredKindForMemkindMalloc;
import static com.hazelcast.internal.memory.impl.MemkindUtil.useMemkind;

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
        PersistentMemoryConfig pmemConfig = config.getPersistentMemoryConfig();
        boolean systemMemMode = pmemConfig.getMode() == SYSTEM_MEMORY;
        assert systemMemMode && pmemConfig.isEnabled() || useMemkind();

        MemkindKind kind = configuredKindForMemkindMalloc(pmemConfig);

        LOGGER.info("Using Memkind memory allocator with " + kind.name() + " kind");
        MemkindHeap heap = MemkindHeap.createHeap(kind, size);
        return new MemkindMalloc(heap);
    }
}
