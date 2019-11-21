package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.cp.internal.persistence.RestoredLogFile.LoadMode.FULL;

class RestoredLogFile {
    private final String filename;
    private final long topIndex;
    private final SnapshotEntry snapshotEntry;
    private final LogEntry[] entries;
    private final long[] entryOffsets;

    enum LoadMode { FULL, JUST_TOP_INDEX }

    RestoredLogFile(@Nonnull String filename, long topIndex) {
        this.filename = filename;
        this.topIndex = topIndex;
        this.snapshotEntry = null;
        this.entries = null;
        this.entryOffsets = null;
    }

    RestoredLogFile(
            @Nonnull String filename,
            @Nullable SnapshotEntry snapshotEntry,
            @Nonnull LogEntry[] entries,
            @Nonnull long[] entryOffsets,
            long topIndex
    ) {
        this.filename = filename;
        this.topIndex = topIndex;
        this.snapshotEntry = snapshotEntry;
        this.entries = entries;
        this.entryOffsets = entryOffsets;
    }

    @Nonnull
    LoadMode loadMode() {
        return entries != null ? FULL : LoadMode.JUST_TOP_INDEX;
    }

    long topIndex() {
        return topIndex;
    }

    @Nullable
    SnapshotEntry snapshotEntry() {
        return snapshotEntry;
    }

    @Nonnull
    LogEntry[] entries() {
        if (entries == null) {
            throw new IllegalStateException("This RestoredLogFile wasn't loaded in the FULL mode");
        }
        return entries;
    }

    @Nonnull
    String filename() {
        return filename;
    }

    @Nonnull
    LogFileStructure toLogFileStructure() {
        if (entryOffsets == null) {
            throw new IllegalStateException("This RestoredLogFile wasn't loaded in the FULL mode");
        }
        return new LogFileStructure(filename, entryOffsets, topIndex - entryOffsets.length + 1);
    }
}
