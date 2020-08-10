package com.hazelcast.internal.memory.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static com.hazelcast.internal.memory.impl.MemkindPmemMalloc.checkPlatform;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.Files.copy;

/**
 * Heap which uses JNI calls to the Memkind library http://memkind.github.io/memkind/
 * to manage memory allocations. This heap can be on non-volatile memory
 * and on volatile memory, with standard and huge page sizes. See the
 * Memkind documentation for the kinds Memkind supports and {@link MemkindKind}
 * for the ones that Hazelcast supports from that list.
 * <p>
 * Only Linux platform is supported.
 *
 * @see MemkindKind
 */
public final class MemkindHeap {

    /**
     * The property allows us to disable non-volatile memory check. Used for internal testing only.
     */
    public static final String PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY =
            "hazelcast.persistent.memory.check.disable";
    /**
     * The flag exists for unit tests only to enable testing in the environment where there is
     * no non-volatile memory installed.
     */
    private static final boolean PERSISTENT_MEMORY_CHECK_ENABLED =
            !"true".equalsIgnoreCase(System.getProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true"));

    private static final ILogger LOGGER = Logger.getLogger(MemkindHeap.class);

    static {
        System.load(extractBundledLib());
    }

    private final Object closeHeapLock = new Object();
    private final long poolHandle;
    private boolean isPoolOpen;

    private MemkindHeap(long poolHandle) {
        this.poolHandle = poolHandle;
        this.isPoolOpen = true;
    }

    /**
     * Creates heap on persistent memory located at {@code path} with the
     * given {@code size}.
     *
     * @param path The path located on persistent memory
     * @param size The size of the heap to create
     * @return the {@link MemkindHeap} instance created
     * @see #createHeap(MemkindKind, long)
     */
    static synchronized MemkindHeap createPmemHeap(String path, long size) {
        Path pmemFilePath = Paths.get(path);
        long heapHandle;

        try {
            heapHandle = createPmemHeap0(path, size);
        } catch (IOException e) {
            LOGGER.severe("Unable to create persistent memory heap at " + pmemFilePath, e);
            deleteFile(pmemFilePath);
            throw new HazelcastException("Unable to create persistent memory heap at " + pmemFilePath.toAbsolutePath(), e);
        }

        if (PERSISTENT_MEMORY_CHECK_ENABLED && !isPmem0(heapHandle)) {
            LOGGER.severe(pmemFilePath.toAbsolutePath()
                    + " is not a persistent memory. Check persistent-memory-directory configuration and restart member again.");
            closeHeap(heapHandle);
            deleteFile(pmemFilePath);
            throw new HazelcastException(pmemFilePath.toAbsolutePath() + " is not a persistent memory");
        }

        return new MemkindHeap(heapHandle);
    }

    /**
     * Creates heap of the requested kind with the given size.
     *
     * @param kind The kind to use for creating the heap
     * @param size The size of the heap to create
     * @return the {@link MemkindHeap} instance created
     * @see #createPmemHeap(String, long)
     */
    static synchronized MemkindHeap createHeap(MemkindKind kind, long size) {
        long heapHandle = createHeap0(kind.ordinal(), size);
        if (heapHandle == NULL_ADDRESS) {
            LOGGER.severe("Unable to create heap with the requested kind " + kind);
            throw new HazelcastException("Unable to create heap with the requested kind " + kind);
        }
        return new MemkindHeap(heapHandle);
    }

    private static void closeHeap(long heapHandle) {
        try {
            closeHeap0(heapHandle);
        } catch (RuntimeException e) {
            LOGGER.fine("Unable to close pool handle");
        }
    }

    void close() {
        synchronized (closeHeapLock) {
            if (isPoolOpen) {
                closeHeap(poolHandle);
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

    void free(long address) {
        free0(poolHandle, address);
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

        try (InputStream resDirIs = MemkindHeap.class.getClassLoader().getResourceAsStream(libraryPath)) {
            Path tmpDir = Files.createTempDirectory("hz_").toAbsolutePath();
            LOGGER.fine(format("Using %s as the temporary directory for the shared libraries to use", tmpDir));
            if (resDirIs == null) {
                throw new RuntimeException("Cannot find native library at : " + libraryPath);
            }

            copySharedLibs(libraryPath, resDirIs, tmpDir);

            return Paths.get(tmpDir.toAbsolutePath().toString(), "libhazelcast-pmem.so").toString();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static void copySharedLibs(String libraryPath, InputStream resDirIs, Path tmpDir) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(resDirIs, US_ASCII))) {
            String resource;
            while ((resource = br.readLine()) != null) {
                String srcPath = Paths.get(libraryPath, resource).toString();
                Path destPath = Paths.get(tmpDir.toString(), resource);
                LOGGER.fine(format("Copying %s to the temporary directory %s", srcPath, destPath));
                try (InputStream srcIs = MemkindHeap.class.getClassLoader().getResourceAsStream(srcPath)) {
                    if (srcIs == null) {
                        throw new RuntimeException("Cannot find native library at : " + srcPath);
                    }
                    copy(srcIs, destPath);
                }
            }
        }
    }

    private static String getBundledLibraryPath() {
        checkPlatform();
        return "libhazelcast-pmem/linux-x86_64/";
    }

    /**
     * Creates persistent memory heap on the specified {@code path} with the
     * given {@code size}.
     *
     * @param path The path the persistent memory heap to locate at
     * @param size The size of the persistent memory heap
     * @return the handle of the created PMEM kind
     * @throws IOException if the heap could not be created
     */
    private static native long createPmemHeap0(String path, long size) throws IOException;

    /**
     * Creates heap of the specified {@code kind} with the given {@code size}.
     *
     * @param kind The kind of the heap to create
     * @param size The size of the heap to create
     * @return the handle of the created heap
     */
    private static native long createHeap0(int kind, long size);

    /**
     * Closes the heap with the given {@code handle} that was created
     * before with the {@link #createPmemHeap0(String, long)} or
     * {@link #createHeap0(int, long)} calls.
     *
     * @param handle The handle of the heap to close
     */
    private static native void closeHeap0(long handle);

    /**
     * Checks if the heap created by the {@link #createPmemHeap0(String, long)}
     * is located on persistent memory or not.
     *
     * @param handle The handle of the persistent memory heap
     * @return {@code true} if the heap is on persistent memory {@code false} otherwise.
     */
    private static native boolean isPmem0(long handle);

    private static native long alloc0(long handle, long size);

    private static native long realloc0(long handle, long address, long size);

    private static native void free0(long handle, long address);
}
