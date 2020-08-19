package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.JVMUtil.is32bitJVM;
import static com.hazelcast.internal.util.OsHelper.OS;
import static com.hazelcast.internal.util.OsHelper.isLinux;

/**
 * LibMalloc implementation for non-volatile memory. Makes JNI calls to the Memkind library
 * for all allocation requests.
 * <p>
 * Creates and locks exclusively the configured persistent memory {@link #directories}.
 */
final class MemkindPmemMalloc implements LibMalloc {
    private static final ILogger LOGGER = Logger.getLogger(MemkindPmemMalloc.class);

    private final List<PersistentMemoryDirectory> directories;
    private final List<MemkindHeap> heaps;

    private MemkindPmemMalloc(List<MemkindHeap> heaps, List<PersistentMemoryDirectory> directories) {
        this.heaps = heaps;
        Preconditions.checkTrue(!heaps.isEmpty(), "Heaps must not be empty");
        this.directories = directories;
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
        try {
            for (PersistentMemoryDirectory directory : pmemDirectories) {
                heaps.add(MemkindHeap.createPmemHeap(directory.getPersistentMemoryFile().getAbsolutePath(), size));
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
                .append(", configured NUMA node: ").append(pmemDir.getNumaNodeId()));
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
        return "PersistentMemoryMalloc";
    }

    @Override
    public long malloc(long size) {
        try {
            return takeHeap().allocate(size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public long realloc(long address, long size) {
        try {
            return takeHeap().realloc(address, size);
        } catch (OutOfMemoryError e) {
            return NULL_ADDRESS;
        }
    }

    @Override
    public void free(long address) {
        takeHeap().free(address);
    }

    private MemkindHeap takeHeap() {
        // TODO multi-heap support (NUMA-aware, round-robin)
        return heaps.get(0);
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
}
