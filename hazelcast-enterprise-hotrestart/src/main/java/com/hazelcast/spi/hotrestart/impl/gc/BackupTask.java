package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.io.DefaultCopyStrategy;
import com.hazelcast.spi.hotrestart.impl.io.FileCopyStrategy;
import com.hazelcast.spi.hotrestart.impl.io.HardLinkCopyStrategy;
import com.hazelcast.util.ExceptionUtil;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;

import static com.hazelcast.nio.IOUtil.closeResource;
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
    /**
     * Property name to determine if hot backup should attempt to use hard links when performing backup. If not possible,
     * it will revert to simple file copy.
     */
    private static final String PROPERTY_BACKUP_USE_HARD_LINKS = "hazelcast.hotrestart.backup.useHardLinks";
    private final File targetDir;
    private final long[] stableTombChunkSeqs;
    private final long[] stableValChunkSeqs;
    private final long maxChunkSeq;
    private final boolean useHardLinks;
    private final String storeName;
    @Inject
    private GcLogger logger;
    @Inject
    private GcHelper gcHelper;
    @Inject
    private ChunkManager chunkManager;
    private FileCopyStrategy copyStrategy;
    private volatile BackupTaskState state = BackupTaskState.NOT_STARTED;

    BackupTask(File targetDir, String storeName, long[] stableValChunkSeqs, long[] stableTombChunkSeqs) {
        Arrays.sort(stableValChunkSeqs);
        Arrays.sort(stableTombChunkSeqs);
        final long maxValChunkSeq = stableValChunkSeqs.length > 0
                ? stableValChunkSeqs[stableValChunkSeqs.length - 1] : Long.MIN_VALUE;
        final long maxTombChunkSeq = stableTombChunkSeqs.length > 0
                ? stableTombChunkSeqs[stableTombChunkSeqs.length - 1] : Long.MIN_VALUE;
        this.targetDir = targetDir;
        this.stableValChunkSeqs = stableValChunkSeqs;
        this.stableTombChunkSeqs = stableTombChunkSeqs;
        this.maxChunkSeq = Math.max(maxValChunkSeq, maxTombChunkSeq);
        final String useHardLinksStr = System.getProperty(PROPERTY_BACKUP_USE_HARD_LINKS);
        this.useHardLinks = useHardLinksStr == null || Boolean.parseBoolean(useHardLinksStr);
        this.storeName = storeName;
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
            copyOrMoveChunks(stableValChunkSeqs, true);
            copyOrMoveChunks(stableTombChunkSeqs, false);
            completedBackup(inProgressFile);
            state = BackupTaskState.SUCCESS;
        } catch (Throwable e) {
            state = BackupTaskState.FAILURE;
            failedBackup(inProgressFile, e);
        } finally {
            // we left the interrupt status unchanged while copying so we clear it here
            Thread.interrupted();
        }
        chunkManager.deletePendingChunks();
        long durationMillis = NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("Backup of hot restart store " + storeName + " finished in " + durationMillis + " ms");
    }

    private void copyOrMoveChunks(long[] seqs, boolean areValChunks) {
        for (long seq : seqs) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            final File chunkSourceFile = stableChunkFile(seq, areValChunks);
            final File chunkTargetDir = ensureDirectory(chunkSourceFile, targetDir);

            if (chunkManager.isChunkPendingDeletion(seq, areValChunks)) {
                rename(chunkSourceFile, new File(chunkTargetDir, chunkSourceFile.getName()));
                chunkManager.removeChunkPendingDeletion(seq, areValChunks);
                continue;
            }
            if (copyStrategy == null && !useHardLinks) {
                copyStrategy = new DefaultCopyStrategy();
            }
            if (copyStrategy != null) {
                copyStrategy.copy(chunkSourceFile, chunkTargetDir);
            } else {
                try {
                    copyStrategy = new HardLinkCopyStrategy();
                    copyStrategy.copy(chunkSourceFile, chunkTargetDir);
                } catch (Exception e) {
                    logger.finest("Failed to use hard links for file copy : " + e.getMessage()
                            + ", cause: " + ExceptionUtil.toString(e));
                    copyStrategy = new DefaultCopyStrategy();
                    copyStrategy.copy(chunkSourceFile, chunkTargetDir);
                }
            }
        }
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

    private File ensureDirectory(File chunkFile, File targetBase) {
        final File targetDir = getTargetDir(chunkFile, targetBase);
        if (!targetDir.exists() && !targetDir.mkdirs()) {
            throw new HazelcastException("Could not create the target directory " + targetDir);
        }
        return targetDir;
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
