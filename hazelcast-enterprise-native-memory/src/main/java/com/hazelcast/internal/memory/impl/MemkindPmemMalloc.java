package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;

import static com.hazelcast.internal.util.JVMUtil.is32bitJVM;
import static com.hazelcast.internal.util.OsHelper.OS;
import static com.hazelcast.internal.util.OsHelper.isLinux;

/**
 * LibMalloc implementation for non-volatile memory. Makes JNI calls to the Memkind library
 * for all allocation requests.
 * <p>
 * Creates and locks exclusively a {@link #directory} backed by a non-volatile memory
 * to store all allocated memory blocks.
 */
final class MemkindPmemMalloc extends AbstractMemkindMalloc {
    private static final ILogger LOGGER = Logger.getLogger(MemkindPmemMalloc.class);

    private final PersistentMemoryDirectory directory;

    MemkindPmemMalloc(MemkindHeap heap, PersistentMemoryDirectory directory) {
        super(heap);
        this.directory = directory;
    }

    static MemkindPmemMalloc create(NativeMemoryConfig config, long size) {
        checkPlatform();
        assert config.getPersistentMemoryDirectory() != null;
        PersistentMemoryDirectory directory = new PersistentMemoryDirectory(config);
        File pmemFile = directory.getPersistentMemoryFile();
        LOGGER.info("Using Memkind PMEM memory allocator with path " + directory.getPersistentMemoryFile().getPath());
        MemkindHeap heap = MemkindHeap.createPmemHeap(pmemFile.getAbsolutePath(), size);
        return new MemkindPmemMalloc(heap, directory);
    }

    @Override
    protected void onDispose() {
        directory.dispose();
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
        return "PersistentMemoryMalloc";
    }
}
