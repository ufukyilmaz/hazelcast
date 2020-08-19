package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;

public final class MemkindUtil {
    public static final String HD_MEMKIND = "hazelcast.hd.memkind";
    public static final String HD_MEMKIND_HUGEPAGES = "hazelcast.hd.memkind.hugepages";
    public static final String HD_MEMKIND_DAX_KMEM = "hazelcast.hd.memkind.dax.kmem";

    private MemkindUtil() {
    }

    public static boolean shouldUseMemkindMalloc(NativeMemoryConfig config) {
        return !config.getPersistentMemoryConfig().getDirectoryConfigs().isEmpty()
                || useMemkind();
    }

    static boolean useMemkind() {
        return "true".equalsIgnoreCase(System.getProperty(HD_MEMKIND));
    }

    static boolean useMemkindHugePages() {
        return "true".equalsIgnoreCase(System.getProperty(HD_MEMKIND_HUGEPAGES));
    }

    static boolean useMemkindDaxKmem() {
        return "true".equalsIgnoreCase(System.getProperty(HD_MEMKIND_DAX_KMEM));
    }
}
