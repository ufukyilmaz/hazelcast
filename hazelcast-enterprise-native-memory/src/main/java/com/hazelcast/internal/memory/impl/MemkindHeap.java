package com.hazelcast.internal.memory.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static com.hazelcast.internal.memory.impl.MemkindPmemMalloc.checkPlatform;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
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
public class MemkindHeap {
    /**
     * The property allows us to disable non-volatile memory check. Used for internal testing only.
     */
    public static final String PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY =
            "hazelcast.persistent.memory.check.disable";

    private static final String LIB_PATH = "libhazelcast-pmem/linux-x86_64/";
    private static final String LIBHAZELCAST_PMEM_SO = "libhazelcast-pmem.so";
    private static final String[] LIBS = new String[]{
            LIBHAZELCAST_PMEM_SO,
            "libmemkind.so.0",
            "libnuma.so.1",
            "libpmem.so.1"
    };
    /**
     * The flag exists for unit tests only to enable testing in the environment where there is
     * no non-volatile memory installed.
     */
    private static final boolean PERSISTENT_MEMORY_CHECK_ENABLED =
            !"true".equalsIgnoreCase(System.getProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "false"));

    private static final ILogger LOGGER = Logger.getLogger(MemkindHeap.class);
    private final int numaNode;
    private final Object closeHeapLock = new Object();
    private final long handle;
    private final String name;
    private boolean isPoolOpen;

    protected MemkindHeap(long handle, int numaNode, String name) {
        this.handle = handle;
        this.numaNode = numaNode;
        this.name = name;
        this.isPoolOpen = true;
    }

    /**
     * Creates heap on persistent memory located at {@code path} with the
     * given {@code size} and NUMA node.
     *
     * @param path     The path located on persistent memory
     * @param size     The size of the heap to create
     * @param numaNode The NUMA node the created heap belongs to
     * @return the {@link MemkindHeap} instance created
     * @see #createHeap(MemkindKind, long)
     */
    static synchronized MemkindHeap createPmemHeap(String path, long size, int numaNode) {
        Path pmemFilePath = Paths.get(path);
        long heapHandle;

        // the pmem file's unique parent folder's parent folder, e.g. /mnt/pmem0
        String pmemDir = parentIfExists(parentIfExists(pmemFilePath)).toString();

        try {
            heapHandle = createPmemHeap0(path, size);
        } catch (IOException e) {
            LOGGER.severe("Unable to create persistent memory heap at " + pmemDir, e);
            deleteFile(pmemFilePath);
            throw new HazelcastException("Unable to create persistent memory heap at " + pmemDir, e);
        }
        if (PERSISTENT_MEMORY_CHECK_ENABLED && !isPmem0(heapHandle)) {
            LOGGER.severe(pmemDir + " is not a persistent memory. Check persistent-memory-directory configuration and restart "
                    + "member again.");
            closeHeap(heapHandle, pmemDir);
            deleteFile(pmemFilePath);
            throw new HazelcastException(pmemFilePath.toAbsolutePath() + " is not a persistent memory");
        }
        return new MemkindHeap(heapHandle, numaNode, pmemDir);
    }

    private static Path parentIfExists(Path path) {
        return path.getParent() != null ? path.getParent() : path;
    }

    /**
     * Creates heap of the requested kind with the given size.
     *
     * @param kind The kind to use for creating the heap
     * @param size The size of the heap to create
     * @return the {@link MemkindHeap} instance created
     * @see #createPmemHeap(String, long, int)
     */
    static synchronized MemkindHeap createHeap(MemkindKind kind, long size) {
        long heapHandle = createHeap0(kind.ordinal(), size);
        if (heapHandle == NULL_ADDRESS) {
            LOGGER.severe("Unable to create heap with the requested kind " + kind);
            throw new HazelcastException("Unable to create heap with the requested kind " + kind);
        }
        return new MemkindHeap(heapHandle, -1, kind.name());
    }

    private static void closeHeap(long heapHandle, String name) {
        try {
            closeHeap0(heapHandle);
            LOGGER.fine("Closed heap " + name);
        } catch (RuntimeException e) {
            LOGGER.fine("Unable to close heap " + name);
        }
    }

    private static void deleteFile(Path filePath) {
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e1) {
            LOGGER.fine("Unable to delete " + filePath);
        }
    }

    private static String extractBundledLib() {
        try {
            Path tmpDir = Files.createTempDirectory("hz_").toAbsolutePath();
            tmpDir.toFile().deleteOnExit();

            String tmpDirStr = tmpDir.toString();
            for (String lib : LIBS) {
                String libraryPath = LIB_PATH + lib;

                try (InputStream src = MemkindHeap.class.getClassLoader().getResourceAsStream(libraryPath)) {
                    if (src == null) {
                        throw new RuntimeException("Cannot find native library at : " + libraryPath);
                    }

                    Path destPath = Paths.get(tmpDirStr, lib);
                    LOGGER.fine(format("Copying %s to the temporary directory %s", libraryPath, destPath));

                    copy(src, destPath);
                    destPath.toFile().deleteOnExit();
                } catch (Throwable t) {
                    throw rethrow(t);
                }
            }

            return Paths.get(tmpDirStr, LIBHAZELCAST_PMEM_SO).toAbsolutePath().toString();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Initializes the native layer, like setting {@code MEMKIND_HOG_MEMORY}
     * environment variable to {@code "1"}.
     *
     * @throws RuntimeException if the initialization  failed
     */
    private static native void init0();

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

    private static native long realloc0(long address, long size);

    private static native void free0(long address);

    String getName() {
        return name;
    }

    int getNumaNode() {
        return numaNode;
    }

    void close() {
        synchronized (closeHeapLock) {
            if (isPoolOpen) {
                closeHeap(handle, name);
                isPoolOpen = false;
            }
        }
    }

    long allocate(long size) {
        return alloc0(handle, size);
    }

    long realloc(long address, long size) {
        if (address == NULL_ADDRESS) {
            // compatibility with Unsafe.reallocateMemory() behavior
            return allocate(size);
        }

        // we don't pass down any poolHandle
        // this makes memkind to detect the kind for the address
        return realloc0(address, size);
    }

    void free(long address) {
        // we don't pass down any poolHandle
        // this makes memkind to detect the kind for the address
        free0(address);
    }

    @Override
    public String toString() {
        return "MemkindHeap{"
                + "name='" + name + '\''
                + ", numaNode=" + numaNode
                + ", handle=" + handle
                + ", isPoolOpen=" + isPoolOpen
                + '}';
    }

    static {
        checkPlatform();
        System.load(extractBundledLib());
        try {
            init0();
        } catch (Exception ex) {
            throw new HazelcastException("Could not initialize properly", ex);
        }
    }
}
