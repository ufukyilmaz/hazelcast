package com.hazelcast.cp.persistence;

import com.hazelcast.cp.internal.raft.exception.LogValidationException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateLoader;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.cp.persistence.BufferedRaf.BufRafObjectDataIn;
import com.hazelcast.cp.persistence.RestoredLogFile.LoadMode;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.MEMBERS_FILENAME;
import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.RAFT_LOG_PREFIX;
import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.TERM_FILENAME;
import static com.hazelcast.cp.persistence.RestoredLogFile.LoadMode.FULL;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class OnDiskRaftStateLoader implements RaftStateLoader {

    private LogFileStructure logFileStructure;

    private static final LogEntry[] EMPTY_LOG_ENTRY_ARRAY = new LogEntry[0];
    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    private final File baseDir;
    private int maxUncommittedEntries;
    private final InternalSerializationService serializationService;

    public OnDiskRaftStateLoader(
            @Nonnull File baseDir,
            int maxUncommittedEntries,
            @Nonnull InternalSerializationService serializationService
    ) {
        this.baseDir = baseDir;
        this.maxUncommittedEntries = maxUncommittedEntries;
        this.serializationService = serializationService;
    }

    @Nonnull @Override
    public RestoredRaftState load() throws IOException {
        checkFileExists(TERM_FILENAME);
        checkFileExists(MEMBERS_FILENAME);

        Tuple2<Integer, RaftEndpoint> termAndVote = readVoteAndTerm();
        Tuple2<RaftEndpoint, Collection<RaftEndpoint>> members = readMembers();

        int term = termAndVote.element1;
        RaftEndpoint votedFor = termAndVote.element2;
        RaftEndpoint localMember = members.element1;
        Collection<RaftEndpoint> initialMembers = members.element2;

        String[] filenames = baseDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(RAFT_LOG_PREFIX);
            }
        });
        if (filenames == null) {
            throw new IOException("Error opening the Raft log directory");
        }
        if (filenames.length == 0) {
            logFileStructure = new LogFileStructure("", EMPTY_LONG_ARRAY, 0);
            return new RestoredRaftState(localMember, initialMembers, term, votedFor, null, EMPTY_LOG_ENTRY_ARRAY);
        }
        RestoredLogFile restored = loadFileWithMostRecentEntry(filenames);
        deleteAllExcept(filenames, restored.filename());
        logFileStructure = restored.toLogFileStructure();
        return new RestoredRaftState(
                localMember, initialMembers, term, votedFor, restored.snapshotEntry(), restored.entries());
    }

    public int maxUncommittedEntries() {
        return maxUncommittedEntries;
    }

    @Nonnull
    public LogFileStructure logFileStructure() {
        return checkNotNull(logFileStructure);
    }

    private void checkFileExists(String fileName) throws IOException {
        File file = new File(baseDir, fileName);
        if (!file.exists() || !file.isFile()) {
            throw new IOException("Error opening the Raft log directory! No " + fileName + " file!");
        }
    }

    @Nonnull
    private RestoredLogFile loadFileWithMostRecentEntry(String[] filenames) throws IOException {
        Arrays.sort(filenames);
        Collections.reverse(Arrays.asList(filenames));
        RestoredLogFile mostRecent = loadFile(filenames[0], FULL);
        for (int i = 1; i < filenames.length; i++) {
            String fname = filenames[i];
            RestoredLogFile rlf = loadFile(fname, LoadMode.JUST_TOP_INDEX);
            if (rlf.topIndex() > mostRecent.topIndex()) {
                mostRecent = rlf;
            }
        }
        return mostRecent.loadMode() == FULL ? mostRecent : loadFile(mostRecent.filename(), FULL);
    }

    private RestoredLogFile loadFile(String fname, LoadMode loadMode) throws IOException {
        BufferedRaf raf = new BufferedRaf(new RandomAccessFile(new File(baseDir, fname), "rw"));
        BufRafObjectDataIn in = raf.asObjectDataInputStream(serializationService);
        try {
            final List<LogEntry> entries = new ArrayList<LogEntry>();
            long topIndex = 0;
            LogEntryRingBuffer entryRingBuffer = null;
            SnapshotEntry snapshotEntry = null;
            while (true) {
                final long entryOffset = raf.filePointer();
                if (entryOffset == raf.length()) {
                    break;
                }
                final LogEntry entry;
                try {
                    entry = in.readObject();
                    in.checkCrc32();
                } catch (Exception e) {
                    raf.seek(entryOffset);
                    raf.setLength(entryOffset);
                    break;
                }
                checkIndexGreaterThanPrevious(entry, topIndex, fname);
                if (entry instanceof SnapshotEntry) {
                    checkSnapshotEntryIsFirst(topIndex, fname);
                    snapshotEntry = (SnapshotEntry) entry;
                } else if (loadMode == FULL) {
                    entries.add(entry);
                    if (entryRingBuffer == null) {
                        entryRingBuffer = new LogEntryRingBuffer(maxUncommittedEntries, entry.index());
                    }
                    entryRingBuffer.addEntryOffset(entryOffset);
                }
                topIndex = entry.index();
            }
            return loadMode == FULL ?
                    new RestoredLogFile(fname,
                            snapshotEntry,
                            entries.toArray(EMPTY_LOG_ENTRY_ARRAY),
                            entryRingBuffer != null ? entryRingBuffer.exportEntryOffsets() : EMPTY_LONG_ARRAY,
                            topIndex)
                    : new RestoredLogFile(fname, topIndex);
        } finally {
            IOUtil.closeResource(raf);
        }
    }

    private static void checkIndexGreaterThanPrevious(LogEntry entry, long topIndex, String fname)
            throws LogValidationException {
        if (entry.index() < topIndex) {
            throw new LogValidationException(String.format(
                    "Invalid entry index in file %s. Top index so far: %,d, now read %,d.",
                    fname, topIndex, entry.index())
            );
        }
    }

    private static void checkSnapshotEntryIsFirst(long topIndex, String fname) throws LogValidationException {
        if (topIndex != 0) {
            throw new LogValidationException("Snapshot entry not at the start of the file " + fname);
        }
    }

    private Tuple2<Integer, RaftEndpoint> readVoteAndTerm() throws IOException {
        Tuple2<Integer, RaftEndpoint> result = runRead(TERM_FILENAME, new ReadTask<Integer, RaftEndpoint>() {
            @Override
            public Tuple2<Integer, RaftEndpoint> readFrom(ObjectDataInput in) throws IOException {
                int term = in.readInt();
                RaftEndpoint votedFor = in.readObject();
                return Tuple2.of(term, votedFor);
            }
        });
        return result != null ? result : Tuple2.of(0, (RaftEndpoint) null);
    }

    private Tuple2<RaftEndpoint, Collection<RaftEndpoint>> readMembers() throws IOException {
        Tuple2<RaftEndpoint, Collection<RaftEndpoint>> result =
            runRead(MEMBERS_FILENAME, new ReadTask<RaftEndpoint, Collection<RaftEndpoint>>() {
                @Override
                public Tuple2<RaftEndpoint, Collection<RaftEndpoint>> readFrom(ObjectDataInput in) throws IOException {
                    RaftEndpoint localMember = in.readObject();
                    Collection<RaftEndpoint> initialMembers = readCollection(in);
                    return Tuple2.of(localMember, initialMembers);
                }
            });
        return result != null ? result : Tuple2.of((RaftEndpoint) null, emptyEndpoints());
    }

    private <T1, T2> Tuple2<T1, T2> runRead(String filename, ReadTask<T1, T2> task) throws IOException {
        File f = new File(baseDir, filename);
        BufferedRaf raf = new BufferedRaf(new RandomAccessFile(f, "r"));
        BufRafObjectDataIn in = raf.asObjectDataInputStream(serializationService);
        try {
            Tuple2<T1, T2> result = task.readFrom(in);
            in.checkCrc32();
            return result;
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private interface ReadTask<T1, T2> {
        Tuple2<T1, T2> readFrom(ObjectDataInput in) throws IOException;
    }

    private void deleteAllExcept(String[] allFilenames, String chosen) {
        for (String fname : allFilenames) {
            if (!fname.equals(chosen)) {
                IOUtil.delete(new File(baseDir, fname));
            }
        }
    }

    private static Collection<RaftEndpoint> emptyEndpoints() {
        return Collections.emptyList();
    }
}
