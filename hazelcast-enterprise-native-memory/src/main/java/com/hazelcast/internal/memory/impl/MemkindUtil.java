package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryMode;

import static com.hazelcast.config.PersistentMemoryMode.SYSTEM_MEMORY;

public final class MemkindUtil {
    public static final String HD_MEMKIND = "hazelcast.hd.memkind";
    public static final String HD_MEMKIND_HUGEPAGES = "hazelcast.hd.memkind.hugepages";

    private MemkindUtil() {
    }

    public static boolean shouldUseMemkindMalloc(NativeMemoryConfig config) {
        PersistentMemoryConfig pmemConfig = config.getPersistentMemoryConfig();
        return pmemConfig.isEnabled() || useMemkind();
    }

    static boolean useMemkind() {
        return "true".equalsIgnoreCase(System.getProperty(HD_MEMKIND));
    }

    static boolean useMemkindHugePages() {
        return "true".equalsIgnoreCase(System.getProperty(HD_MEMKIND_HUGEPAGES));
    }

    /**
     * Utility method for kind selection. Used by {@link MemkindMalloc#create(NativeMemoryConfig, long)}.
     * The method should be called only if the {@link MemkindMalloc} is created by
     * {@link MemkindMallocFactory#create(long)}, therefore, it is expected {@link PersistentMemoryConfig#getMode()}
     * is set to {@link PersistentMemoryMode#SYSTEM_MEMORY} if {@link PersistentMemoryConfig#isEnabled()} returns
     * {@code true}. Otherwise, {@link IllegalStateException} is thrown.
     *
     * @param pmemConfig The persistent memory configuration
     * @return The chosen Memkind kind
     */
    static MemkindKind configuredKindForMemkindMalloc(PersistentMemoryConfig pmemConfig) {

        if (!useMemkind() && pmemConfig.isEnabled() && SYSTEM_MEMORY == pmemConfig.getMode()) {
            return MemkindKind.PMEM_DAX_KMEM;
        }

        if (useMemkind()) {
            if (useMemkindHugePages()) {
                return MemkindKind.DRAM_HUGEPAGES;
            }

            return MemkindKind.DRAM;
        }

        throw new IllegalStateException("Could not determine the Memkind kind");
    }
}
