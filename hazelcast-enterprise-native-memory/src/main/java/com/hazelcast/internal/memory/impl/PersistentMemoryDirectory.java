package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.DirectoryLock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.util.UuidUtil;

import java.io.File;
import java.util.UUID;

import static com.hazelcast.internal.util.DirectoryLock.lockForDirectory;

/**
 * Manages persistent memory directory used by {@link PersistentMemoryMalloc}.
 * <p>
 * Locks the directory and creates new file for {@link PersistentMemoryHeap}.
 */
final class PersistentMemoryDirectory {

    private static final ILogger LOGGER = Logger.getLogger(PersistentMemoryDirectory.class);

    private static final String PERSISTENT_MEMORY_FILE_EXTENSION = ".pmem";
    private static final int LOCK_NEW_PMEM_DIRECTORY_ATTEMPTS_NUMBER = 5;

    /**
     * A directory lock which is acquired in the constructor and released only when the
     * directory is disposed. The lock is released automatically on JVM crash.
     */
    private DirectoryLock directoryLock;

    /**
     * Native memory configuration.
     */
    private final NativeMemoryConfig config;

    /**
     * The File where {@link PersistentMemoryHeap} allocates memory.
     * The file backs non-volatile Optane memory.
     */
    private final File pmemFile;

    PersistentMemoryDirectory(NativeMemoryConfig config) {
        this.config = config;
        this.pmemFile = acquireDirectory();
    }

    File getPersistentMemoryFile() {
        return pmemFile;
    }

    private File acquireDirectory() {
        File pmemDirectory = new File(config.getPersistentMemoryDirectory());
        if (!pmemDirectory.exists() && !pmemDirectory.mkdirs() && !pmemDirectory.exists()) {
            throw new HazelcastException("Could not create " + pmemDirectory.getAbsolutePath());
        }
        if (!pmemDirectory.isDirectory()) {
            throw new HazelcastException(pmemDirectory.getAbsolutePath() + " is not a directory!");
        }

        File[] dirs = pmemDirectory.listFiles(f -> {
            return f.isDirectory() && isUUID(f.getName());
        });

        if (dirs == null) {
            return newDirectoryAndDatafile(pmemDirectory, false);
        }
        for (File dir : dirs) {
            try {
                return newDirectoryAndDatafile(dir, true);
            } catch (Exception e) {
                LOGGER.fine("Could not lock persistent memory directory: " + dir.getAbsolutePath()
                        + ". Reason: " + e.getMessage());
            }
        }
        return newDirectoryAndDatafile(pmemDirectory, false);
    }

    private File newDirectoryAndDatafile(File pmemDirectory, boolean reuseDirectory) {
        directoryLock = reuseDirectory ? lockForDirectory(pmemDirectory, LOGGER)
                : newDirectory(pmemDirectory);
        clearDirectory(directoryLock.getDir());
        return newDatafile();
    }

    private static DirectoryLock newDirectory(File pmemDirectory) {
        for (int attempt = 1; attempt <= LOCK_NEW_PMEM_DIRECTORY_ATTEMPTS_NUMBER; ++attempt) {
            File dir = new File(pmemDirectory, UuidUtil.newUnsecureUuidString());
            boolean created = dir.mkdir();
            if (!created) {
                throw new HazelcastException("Unable to create " + dir);
            }
            LOGGER.info("Created new empty persistent memory directory: " + dir.getAbsolutePath());
            try {
                DirectoryLock lock = lockForDirectory(dir, LOGGER);
                return lock;
            } catch (Exception e) {
                // Unable to lock our own directory.
                // Concurrent process may try to reuse it.
                LOGGER.fine("Could not lock persistent memory directory " + dir.getAbsolutePath()
                        + ". Reason: " + e.getMessage());

                // Failed to create and lock new directory
                if (attempt >= LOCK_NEW_PMEM_DIRECTORY_ATTEMPTS_NUMBER) {
                    throw e;
                }
            }
        }
        throw new HazelcastException("Unable to create persitent memory directory at " + pmemDirectory);
    }

    private File newDatafile() {
        return new File(directoryLock.getDir(), UuidUtil.newUnsecureUuidString() + PERSISTENT_MEMORY_FILE_EXTENSION);
    }

    private static void clearDirectory(File directory) {
        File[] files = directory.listFiles(file -> file.getName().endsWith(PERSISTENT_MEMORY_FILE_EXTENSION));

        if (files != null) {
            for (File file : files) {
                boolean deleted = file.delete();
                if (!deleted) {
                    LOGGER.fine("Could not delete file " + file.getAbsolutePath());
                }
            }
        }
    }

    void dispose() {
        clearDirectory(directoryLock.getDir());
        directoryLock.release();
    }

    private static boolean isUUID(String filename) {
        try {
            UUID.fromString(filename);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

}
