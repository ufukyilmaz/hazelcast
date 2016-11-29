package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.copyFile;
import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.nio.IOUtil.rename;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Class in charge of copying stable hot restart chunks. Creates {@link #IN_PROGRESS_FILE} while progressing and the
 * {@link #FAILURE_FILE} file in case of failure.
 */
public class BackupTask implements Runnable {
    /** Name of the file which indicates that there is a backup in progress */
    public static final String IN_PROGRESS_FILE = "inprogress";
    /** Name of the file which indicates that there was a failure during backup */
    public static final String FAILURE_FILE = "failure";
    private final File targetDir;
    private final long[] stableTombChunkSeqs;
    private final long[] stableValChunkSeqs;
    private final long maxChunkSeq;
    @Inject
    private GcLogger logger;
    @Inject
    private GcHelper gcHelper;
    @Inject
    private ChunkManager chunkManager;
    private volatile BackupTaskState state = BackupTaskState.NOT_STARTED;

    BackupTask(File targetDir, long[] stableValChunkSeqs, long[] stableTombChunkSeqs) {
        Arrays.sort(stableValChunkSeqs);
        Arrays.sort(stableTombChunkSeqs);
        final long maxValChunkSeq = stableValChunkSeqs[stableValChunkSeqs.length - 1];
        final long maxTombChunkSeq = stableTombChunkSeqs[stableTombChunkSeqs.length - 1];
        this.targetDir = targetDir;
        this.stableValChunkSeqs = stableValChunkSeqs;
        this.stableTombChunkSeqs = stableTombChunkSeqs;
        this.maxChunkSeq = Math.max(maxValChunkSeq, maxTombChunkSeq);
    }

    /**
     * Runs the backup task. This task will copy only stable chunks with the given sequences. The backup task will first
     * create a new file with the name {@link #IN_PROGRESS_FILE} and start copying the chunks. When finished successfully or with
     * a failure, the file will be removed. In case of failure, a new file with the name {@link #FAILURE_FILE} will be created
     * with the exception stack trace.
     */
    public void run() {
        state = BackupTaskState.IN_PROGRESS;
        final long start = System.nanoTime();
        final File inProgressFile = new File(targetDir, IN_PROGRESS_FILE);
        try {
            if (!inProgressFile.createNewFile()) {
                throw new IllegalStateException("Hot restart backup is currently not running but the " + IN_PROGRESS_FILE
                        + " file exists. Changing the file to mark failed backup");
            }
        } catch (Exception e) {
            failedBackup(inProgressFile, e);
        }
        int valChunkArrayIdx = 0;
        int tombChunkArrayIdx = 0;
        try {
            while (!Thread.interrupted()
                    && (valChunkArrayIdx < stableValChunkSeqs.length || tombChunkArrayIdx < stableTombChunkSeqs.length)) {
                final boolean hasTombChunks = tombChunkArrayIdx < stableTombChunkSeqs.length;
                final boolean hasValChunks = valChunkArrayIdx < stableValChunkSeqs.length;

                final boolean nextChunkIsVal = !hasTombChunks
                        || (hasValChunks && stableValChunkSeqs[valChunkArrayIdx] < stableTombChunkSeqs[tombChunkArrayIdx]);
                final long nextChunkSeq = nextChunkIsVal ? stableValChunkSeqs[valChunkArrayIdx++]
                        : stableTombChunkSeqs[tombChunkArrayIdx++];
                final File chunkFile = stableChunkFile(nextChunkSeq, nextChunkIsVal);

                if (chunkManager.isChunkPendingDeletion(nextChunkSeq, nextChunkIsVal)) {
                    moveChunk(chunkFile, targetDir);
                    chunkManager.removeChunkPendingDeletion(nextChunkSeq, nextChunkIsVal);
                } else {
                    copyChunk(chunkFile, targetDir, -1);
                }
            }
            completedBackup(inProgressFile);
            state = BackupTaskState.SUCCESS;
        } catch (Throwable e) {
            state = BackupTaskState.FAILURE;
            failedBackup(inProgressFile, e);
        }
        chunkManager.deletePendingChunks();
        logger.finest("Hot restart backup finished in " + NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private void completedBackup(File inProgressFile) {
        deleteQuietly(inProgressFile);
    }

    private void failedBackup(File inProgressFile, Throwable e) {
        logger.warning("Hot restart store backup failed", e);
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(inProgressFile, "UTF-8");
            e.printStackTrace(writer);
        } catch (Throwable ex) {
            logger.warning("Error occurred while generating backup error file", ex);
        } finally {
            closeResource(writer);
        }
        rename(inProgressFile, new File(inProgressFile.getParentFile(), FAILURE_FILE));
    }

    private void copyChunk(File chunkFile, File targetBase, long sourceCount) throws IOException {
        final File targetDir = getTargetDir(chunkFile, targetBase);
        copyFile(chunkFile, targetDir, sourceCount);
    }

    private void moveChunk(File chunkFile, File targetBase) {
        final File targetDir = getTargetDir(chunkFile, targetBase);
        if (!targetDir.exists() && !targetDir.mkdirs()) {
            throw new HazelcastException("Could not create the target directory " + targetDir);
        }
        rename(chunkFile, new File(targetDir, chunkFile.getName()));
    }

    private static File getTargetDir(File chunkFile, File targetBase) {
        final File bucket = chunkFile.getParentFile();
        final File base = bucket.getParentFile();
        return new File(new File(targetBase, base.getName()), bucket.getName());
    }

    private File stableChunkFile(long seq, boolean valChunk) {
        final String baseDir = valChunk ? VAL_BASEDIR : TOMB_BASEDIR;
        final File file = gcHelper.chunkFile(baseDir, seq, Chunk.FNAME_SUFFIX, false);
        if (file.exists()) {
            return file;
        }
        throw new IllegalStateException("Could not find stable chunk file for chunk " + seq);
    }

    /** Returns the current state of the backup task */
    public BackupTaskState getBackupState() {
        return state;
    }

    public long getMaxChunkSeq() {
        return maxChunkSeq;
    }
}
