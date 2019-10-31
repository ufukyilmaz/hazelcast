package com.hazelcast.cp.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.persistence.BufferedRaf.BufRafObjectDataOut;
import com.hazelcast.cp.persistence.FileIOSupport.Writable;
import com.hazelcast.internal.serialization.InternalSerializationService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;

import static com.hazelcast.cp.persistence.FileIOSupport.writeWithChecksum;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static java.lang.Math.max;

/**
 * Disk based implementation of {@link RaftStateStore}.
 */
public class OnDiskRaftStateStore implements RaftStateStore {

    static final String RAFT_LOG_PREFIX = "raftlog-";
    static final String MEMBERS_FILENAME = "members";
    static final String TERM_FILENAME = "term";

    private final File baseDir;
    private final InternalSerializationService serializationService;
    private final LogEntryRingBuffer logEntryRingBuffer;
    private BufferedRaf logRaf;
    private BufRafObjectDataOut logDataOut;
    private boolean flushCalledOnCurrFile;
    private File currentFile;
    private File danglingFile;
    private long nextEntryIndex;

    public OnDiskRaftStateStore(
            @Nonnull File baseDir,
            @Nonnull InternalSerializationService serializationService,
            int maxUncommittedEntries,
            @Nullable LogFileStructure logFileStructure
    ) {
        this.baseDir = baseDir;
        this.serializationService = serializationService;
        if (logFileStructure != null) {
            long[] tailEntryOffsets = logFileStructure.tailEntryOffsets();
            this.nextEntryIndex = logFileStructure.indexOfFirstTailEntry() + tailEntryOffsets.length;
            this.logEntryRingBuffer = new LogEntryRingBuffer(maxUncommittedEntries, logFileStructure);
            this.currentFile = new File(baseDir, logFileStructure.filename());
        } else {
            this.nextEntryIndex = 1;
            this.logEntryRingBuffer = new LogEntryRingBuffer(maxUncommittedEntries);
        }
    }

    @Override
    public void open() throws IOException {
        if (!baseDir.exists() && !baseDir.mkdir() && !baseDir.exists()) {
            throw new IOException("Cannot create directory " + baseDir.getAbsolutePath());
        }
        if (currentFile == null) {
            currentFile = fileWithIndex(nextEntryIndex);
        }
        logRaf = openForAppend(currentFile);
        logDataOut = newObjectDataOutput(logRaf);
    }

    @Override
    public void persistEntry(@Nonnull LogEntry entry) throws IOException {
        if (entry.index() != nextEntryIndex) {
            throw new IllegalArgumentException(String.format(
                    "Expected entry index %,d, but got %,d (%s)", nextEntryIndex, entry.index(), entry.toString()));
        }
        logEntryRingBuffer.addEntryOffset(logRaf.filePointer());
        logDataOut.writeObject(entry);
        logDataOut.writeCrc32();
        nextEntryIndex++;
    }

    @Override
    public void persistSnapshot(@Nonnull SnapshotEntry snapshot) throws IOException {
        File newFile = fileWithIndex(snapshot.index());
        BufferedRaf newRaf = openForAppend(newFile);
        BufRafObjectDataOut newDataOut = newObjectDataOutput(newRaf);
        newDataOut.writeObject(snapshot);
        newDataOut.writeCrc32();
        long newStartOffset = newRaf.filePointer();
        if (logEntryRingBuffer.topIndex() > snapshot.index()) {
            long copyFromOffset = logEntryRingBuffer.getEntryOffset(snapshot.index() + 1);
            logRaf.seek(copyFromOffset);
            logRaf.copyTo(newRaf);
        }
        logRaf.close();
        logRaf = newRaf;
        logDataOut = newDataOut;
        logEntryRingBuffer.adjustToNewFile(newStartOffset, snapshot.index());
        nextEntryIndex = max(nextEntryIndex, snapshot.index() + 1);
        if (flushCalledOnCurrFile) {
            danglingFile = currentFile;
            flushCalledOnCurrFile = false;
        } else {
            delete(currentFile);
        }
        currentFile = newFile;
    }

    @Override
    public void deleteEntriesFrom(long startIndexInclusive) throws IOException {
        long rollbackOffset = logEntryRingBuffer.deleteEntriesFrom(startIndexInclusive);
        logRaf.seek(rollbackOffset);
        logRaf.setLength(rollbackOffset);
        nextEntryIndex = startIndexInclusive;
    }

    @Override
    public void persistInitialMembers(
            @Nonnull final RaftEndpoint localMember,
            @Nonnull final Collection<RaftEndpoint> initialMembers
    ) throws IOException {
        runWrite(MEMBERS_FILENAME, out -> {
            out.writeObject(localMember);
            writeCollection(initialMembers, out);
        });
    }

    @Override
    public void persistTerm(final int term, @Nullable final RaftEndpoint votedFor) throws IOException {
        runWrite(TERM_FILENAME, out -> {
            out.writeInt(term);
            out.writeObject(votedFor);
        });
    }

    @Override
    public void flushLogs() throws IOException {
        logDataOut.flush();
        logRaf.force();
        flushCalledOnCurrFile = true;
        if (danglingFile != null) {
            delete(danglingFile);
            danglingFile = null;
        }
    }

    @Override
    public void close() throws IOException {
        flushLogs();
        logRaf.close();
    }

    @Nonnull
    private BufRafObjectDataOut newObjectDataOutput(BufferedRaf bufRaf) {
        return bufRaf.asObjectDataOutputStream(serializationService);
    }

    @Nonnull
    private File fileWithIndex(long entryIndex) {
        File newFile = new File(baseDir, getRaftLogFileName(entryIndex));
        if (currentFile != null && currentFile.getName().equals(newFile.getName())) {
            throw new IllegalArgumentException("invalid index: " + entryIndex + " for new file!");
        }

        return newFile;
    }

    private void runWrite(String filename, Writable writable) throws IOException {
        writeWithChecksum(baseDir, filename, serializationService, writable);
    }

    @Nonnull
    private static BufferedRaf openForAppend(File file) throws IOException {
        BufferedRaf raf = new BufferedRaf(new RandomAccessFile(file, "rw"));
        raf.seek(raf.length());
        return raf;
    }

    @Nonnull
    static String getRaftLogFileName(long entryIndex) {
        return String.format(RAFT_LOG_PREFIX + "%016x", entryIndex);
    }
}
