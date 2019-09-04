package com.hazelcast.internal.memory.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.internal.memory.impl.PersistentMemoryMalloc.checkPlatform;
import static com.hazelcast.nio.IOUtil.copy;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Persistent memory heap which uses JNI calls to the libvmem PMDK library https://github.com/pmem/pmdk
 * to manage memory allocations.
 * <p>
 * The creation of heap fails if the configured persistent-memory-directory doesn't point to the
 * non-volatile memory.
 * <p>
 * Only Linux platform is supported.
 */
public final class PersistentMemoryHeap {

    /**
     * The property allows us to disable non-volatile memory check. Used for internal testing only.
     */
    public static final String PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY  =
            "hazelcast.persistent.memory.check.disable";
    /**
     * The flag exists for unit tests only to enable testing in the environment where there is
     * no non-volatile memory installed.
     */
    private static final boolean PERSISTENT_MEMORY_CHECK_ENABLED =
            System.getProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY) == null;

    private static final ILogger LOGGER = Logger.getLogger(PersistentMemoryHeap.class);

    static {
        System.load(extractBundledLib());
    }

    private final Object closeHeapLock = new Object();
    private final Path path;
    private final long poolHandle;
    private boolean isPoolOpen;

    private PersistentMemoryHeap(Path path, long poolHandle) {
        this.path = path;
        this.poolHandle = poolHandle;
        this.isPoolOpen = true;
    }

    static synchronized PersistentMemoryHeap createHeap(String path, long size) {
        Path pmemFilePath = Paths.get(path);
        long heapHandle;

        try {
            heapHandle = createHeap0(path, size);
        } catch (IOException e) {
            LOGGER.severe("Unable to create persistent memory heap at " + pmemFilePath, e);
            deleteFile(pmemFilePath);
            throw new HazelcastException("Unable to create persistent memory heap at " + pmemFilePath.toAbsolutePath(), e);
        }

        if (PERSISTENT_MEMORY_CHECK_ENABLED && !isPmem0(heapHandle)) {
            LOGGER.severe(pmemFilePath.toAbsolutePath()
                    + " is not a persistent memory. Check persistent-memory-directory configuration and restart member again.");
            deleteFile(pmemFilePath);
            throw new HazelcastException(pmemFilePath.toAbsolutePath() + " is not a persistent memory");
        }

        return new PersistentMemoryHeap(pmemFilePath, heapHandle);
    }

    void close() {
        synchronized (closeHeapLock) {
            if (isPoolOpen) {
                try {
                    closeHeap0(poolHandle);
                } catch (IOException e) {
                    LOGGER.fine("Unable to close pool handle");
                } finally {
                    deleteFile(path);
                }

                isPoolOpen = false;
            }
        }
    }

    long allocate(long size) {
        return alloc0(poolHandle, size);
    }

    long realloc(long address, long size) {
        return realloc0(poolHandle, address, size);
    }

    long free(long address) {
        return free0(poolHandle, address);
    }

    private static void deleteFile(Path filePath) {
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e1) {
            LOGGER.fine("Unable to delete " + filePath);
        }
    }

    private static String extractBundledLib() {
        String libraryPath = getBundledLibraryPath();

        try (InputStream src = PersistentMemoryHeap.class.getClassLoader().getResourceAsStream(libraryPath)) {
            if (src == null) {
                throw new RuntimeException("Cannot find native libray at : " + libraryPath);
            }

            File dest = File.createTempFile("hazelcast-libpmdk-", ".so");
            dest.deleteOnExit();

            copy(src, dest);

            return dest.getAbsolutePath();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static String getBundledLibraryPath() {
        checkPlatform();
        return "libpmdk/linux-x86_64/libpmdk.so";
    }

    private static native long createHeap0(String path, long size) throws IOException;

    private static native void closeHeap0(long handle) throws IOException;

    private static native boolean isPmem0(long handle);

    private static native long alloc0(long handle, long size);

    private static native long realloc0(long handle, long address, long size);

    private static native long free0(long handle, long address);
}
