package com.hazelcast.spi.hotrestart;

import com.hazelcast.logging.ILogger;
import com.hazelcast.util.EmptyStatement;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * A DirectoryLock represents a lock on a specific directory.
 * <p>
 * DirectoryLock is acquired by calling {@link #lockForDirectory(File, ILogger)}.
 */
final class DirectoryLock {

    private static final String FILE_NAME = "lock";

    private final File dir;
    private final FileChannel channel;
    private final FileLock lock;
    private final ILogger logger;

    private DirectoryLock(File dir, FileChannel channel, FileLock lock, ILogger logger) {
        this.dir = dir;
        this.channel = channel;
        this.lock = lock;
        this.logger = logger;
    }

    /**
     * Returns the locked directory.
     */
    File getDir() {
        return dir;
    }

    /**
     * Returns the actual {@link FileLock}.
     */
    FileLock getLock() {
        return lock;
    }

    /**
     * Releases the lock on directory.
     */
    void release() {
        if (logger.isFineEnabled()) {
            logger.fine("Releasing lock on " + lockFile().getAbsolutePath());
        }
        try {
            lock.release();
        } catch (ClosedChannelException e) {
            EmptyStatement.ignore(e);
        } catch (IOException e) {
            logger.severe("Problem while releasing the lock on " + lockFile(), e);
        }
        try {
            channel.close();
        } catch (IOException e) {
            logger.severe("Problem while closing the channel " + lockFile(), e);
        }
    }

    private File lockFile() {
        return new File(dir, FILE_NAME);
    }

    /**
     * Acquires a lock for given directory. A special file, named <strong>lock</strong>,
     * is created inside the directory and that file is locked.
     *
     * @param dir    the directory
     * @param logger logger
     * @throws HotRestartException If lock file cannot be created or it's already locked
     */
    static DirectoryLock lockForDirectory(File dir, ILogger logger) {
        File lockFile = new File(dir, FILE_NAME);
        FileChannel channel = openChannel(lockFile);
        FileLock lock = acquireLock(lockFile, channel);
        if (logger.isFineEnabled()) {
            logger.fine("Acquired lock on " + lockFile.getAbsolutePath());
        }
        return new DirectoryLock(dir, channel, lock, logger);
    }

    private static FileChannel openChannel(File lockFile) {
        try {
            return new RandomAccessFile(lockFile, "rw").getChannel();
        } catch (IOException e) {
            throw new HotRestartException("Cannot create lock file " + lockFile.getAbsolutePath(), e);
        }
    }

    private static FileLock acquireLock(File lockFile, FileChannel channel) {
        FileLock fileLock = null;
        try {
            fileLock = channel.tryLock();
            if (fileLock == null) {
                throw new HotRestartException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                        + ". Directory is already being used by another member.");
            }
            return fileLock;
        } catch (OverlappingFileLockException e) {
            throw new HotRestartException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                    + ". Directory is already being used by another member.", e);
        } catch (IOException e) {
            throw new HotRestartException("Unknown failure while acquiring lock on " + lockFile.getAbsolutePath(), e);
        } finally {
            if (fileLock == null) {
                closeResource(channel);
            }
        }
    }
}