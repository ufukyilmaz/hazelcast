package com.hazelcast.cp.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RestoreSnapshotRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftGroupId;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.HdrHistogram.Histogram;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.RAFT_LOG_PREFIX;
import static com.hazelcast.cp.persistence.OnDiskRaftStateStore.getRaftLogFileName;
import static com.hazelcast.internal.nio.IOUtil.copy;
import static com.hazelcast.internal.nio.IOUtil.rename;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OnDiskRaftStateStoreTest {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule(true);

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    private int maxUncommittedEntries = 10 * 1000;

    private File baseDir;

    private OnDiskRaftStateStore store;

    private List<RaftEndpoint> members;

    @Before
    public void init() throws IOException {
        baseDir = hotRestartFolderRule.getBaseDir();
        assertTrue(baseDir.exists() && baseDir.isDirectory());
        store = openNewStore(null);

        store.persistTerm(1, null);
        TestRaftEndpoint endpoint1 = newRaftMember(5000);
        members = Arrays.<RaftEndpoint>asList(endpoint1, newRaftMember(5001), newRaftMember(5002));
        store.persistInitialMembers(endpoint1, members);
    }

    @Ignore
    @Test
    public void benchmark() throws Exception {
        int entryCount = maxUncommittedEntries * 1000;
        int entriesAfterSnapshot = maxUncommittedEntries - 2;
        byte[] payload = new byte[100];
        Arrays.fill(payload, (byte) 42);
        Histogram histo = new Histogram(3);
        System.out.println("Writing");
        for (int i = 1; i <= entryCount; i++) {
            long start = System.nanoTime();
            store.persistEntry(new LogEntry(1, i, new ApplyRaftRunnable(payload)));
            if (i % maxUncommittedEntries == 0) {
                System.out.format("Snapshot %,d%n", i);
                store.persistSnapshot(new SnapshotEntry(1, i - entriesAfterSnapshot,
                        new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 3L, "snapshot"),
                        0, members));
                store.flushLogs();
            }
            long end = System.nanoTime();
            histo.recordValue(NANOSECONDS.toMicros(end - start));
        }
        histo.outputPercentileDistribution(System.out, 1.0);
//        System.out.println("Reading");
//        int iterations = 100;
//        for (int i = 0; i < iterations; i++) {
//            RestoredRaftState restoredState = restoreState();
//            LogEntry[] entries = restoredState.entries();
//            assertNotNull(entries);
//            assertEquals(entriesAfterSnapshot, entries.length);
//        }
    }

    @Test
    public void when_singleEntryPersisted_then_entryLoaded() throws IOException {
        // When
        store.persistEntry(newLogEntry(1));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(1, entries.length);
        LogEntry entry = entries[0];
        assertEquals(1, entry.term());
        assertEquals(1, entry.index());
        ApplyRaftRunnable runnable = (ApplyRaftRunnable) entry.operation();
        assertEquals("val1", runnable.getVal());
    }

    @Test
    public void when_multipleEntriesPersisted_then_entriesLoaded() throws IOException {
        // Given
        int entryCount = maxUncommittedEntries * 10;

        // When
        for (int i = 1; i <= entryCount; i++) {
            store.persistEntry(newLogEntry(i));
        }
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(entryCount, entries.length);
        for (int i = 1; i <= entryCount; i++) {
            LogEntry entry = entries[i - 1];
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
            ApplyRaftRunnable runnable = (ApplyRaftRunnable) entry.operation();
            assertEquals("val" + i, runnable.getVal());
        }
    }

    @Test
    public void when_givenEntryIndexIsSmallerThanNextEntryIndex_then_fail() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));

        // When-Then
        exceptionRule.expect(IllegalArgumentException.class);
        store.persistEntry(newLogEntry(1));
    }

    @Test
    public void when_givenEntryIndexIsGreaterThanNextEntryIndex_then_fail() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));

        // When-Then
        exceptionRule.expect(IllegalArgumentException.class);
        store.persistEntry(newLogEntry(3));
    }

    @Test
    public void when_multipleEntriesPersisted_then_entriesDeletedFromBottomIndex() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));

        // When
        store.deleteEntriesFrom(1);
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(0, entries.length);
    }

    @Test
    public void when_multipleEntriesPersisted_then_entriesDeletedFromMiddleIndex() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));

        // When
        store.deleteEntriesFrom(3);
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        LogEntry entry1 = entries[0];
        assertEquals(1, entry1.term());
        assertEquals(1, entry1.index());
        ApplyRaftRunnable runnable1 = (ApplyRaftRunnable) entry1.operation();
        assertEquals("val1", runnable1.getVal());
        LogEntry entry2 = entries[1];
        assertEquals(1, entry2.term());
        assertEquals(2, entry2.index());
        ApplyRaftRunnable runnable2 = (ApplyRaftRunnable) entry2.operation();
        assertEquals("val2", runnable2.getVal());
    }

    @Test
    public void when_multipleEntriesPersisted_then_entriesDeletedFromTopIndex() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));

        // When
        store.deleteEntriesFrom(3);
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        LogEntry entry1 = entries[0];
        assertEquals(1, entry1.term());
        assertEquals(1, entry1.index());
        ApplyRaftRunnable runnable1 = (ApplyRaftRunnable) entry1.operation();
        assertEquals("val1", runnable1.getVal());
        LogEntry entry2 = entries[1];
        assertEquals(1, entry2.term());
        assertEquals(2, entry2.index());
        ApplyRaftRunnable runnable2 = (ApplyRaftRunnable) entry2.operation();
        assertEquals("val2", runnable2.getVal());
    }

    @Test
    public void when_multipleEntriesPersisted_then_entriesCannotBeDeletedFromCommittedIndex() throws IOException {
        // Given
        for (int i = 1; i <= maxUncommittedEntries; i++) {
            store.persistEntry(newLogEntry(i));
        }
        store.persistEntry(newLogEntry(maxUncommittedEntries + 1));

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        store.deleteEntriesFrom(1);
    }

    @Test
    public void when_notPersistedEntryDeleted_then_fail() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        store.deleteEntriesFrom(2);
    }

    @Test
    public void when_snapshotPersistedWithFutureIndex_then_allPreviousEntriesDiscarded() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));

        // When
        store.persistSnapshot(newSnapshotEntry(5));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(0, entries.length);
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(5, snapshotEntry.index());
        assertEquals(0, snapshotEntry.groupMembersLogIndex());
        assertEquals(members, new ArrayList<>(snapshotEntry.groupMembers()));
        RestoreSnapshotRaftRunnable restored = (RestoreSnapshotRaftRunnable) snapshotEntry.operation();
        assertEquals("snapshotAt5", restored.getSnapshot());
        assertSingleRaftLogFile(5);
    }

    @Test
    public void when_snapshotPersistedWithTopIndex_then_allPreviousEntriesDiscarded() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));

        // When
        store.persistSnapshot(newSnapshotEntry(5));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(0, entries.length);
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(5, snapshotEntry.index());
        assertEquals(0, snapshotEntry.groupMembersLogIndex());
        assertEquals(members, new ArrayList<>(snapshotEntry.groupMembers()));
        RestoreSnapshotRaftRunnable restored = (RestoreSnapshotRaftRunnable) snapshotEntry.operation();
        assertEquals("snapshotAt5", restored.getSnapshot());
        assertSingleRaftLogFile(5);
    }

    @Test
    public void when_snapshotPersistedWithMiddleIndex_then_allPreviousEntriesDiscarded() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));

        // When
        store.persistSnapshot(newSnapshotEntry(3));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        LogEntry entry1 = entries[0];
        assertEquals(1, entry1.term());
        assertEquals(4, entry1.index());
        LogEntry entry2 = entries[1];
        assertEquals(1, entry2.term());
        assertEquals(5, entry2.index());
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(3, snapshotEntry.index());
        assertSingleRaftLogFile(3);
    }

    @Test
    public void when_snapshotPersistedWithBottomIndex_then_onlyBottomEntryDiscarded() throws IOException {
        // Given
        for (int i = 1; i <= maxUncommittedEntries; i++) {
            store.persistEntry(newLogEntry(i));
        }
        store.persistEntry(newLogEntry(maxUncommittedEntries + 1));

        // When
        store.persistSnapshot(newSnapshotEntry(2));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        int remainingEntryCountAfterSnapshot = maxUncommittedEntries - 1;
        assertEquals(remainingEntryCountAfterSnapshot, entries.length);
        for (int i = 0; i < remainingEntryCountAfterSnapshot; i++) {
            LogEntry entry = entries[i];
            assertEquals(1, entry.term());
            assertEquals(3 + i, entry.index());

        }
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(2, snapshotEntry.index());
        assertSingleRaftLogFile(2);
    }

    @Test
    public void when_snapshotPersistedWithInvalidIndex_then_fail() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));

        // When-Then
        exceptionRule.expect(IllegalArgumentException.class);
        store.persistSnapshot(newSnapshotEntry(1));
    }

    @Test
    public void when_newEntriesPersistedAfterSnapshot_then_newEntriesLoaded() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));
        store.persistSnapshot(newSnapshotEntry(3));

        // When
        store.persistEntry(newLogEntry(6));
        store.persistEntry(newLogEntry(7));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(4, entries.length);
        LogEntry entry1 = entries[0];
        assertEquals(1, entry1.term());
        assertEquals(4, entry1.index());
        LogEntry entry2 = entries[1];
        assertEquals(1, entry2.term());
        assertEquals(5, entry2.index());
        LogEntry entry3 = entries[2];
        assertEquals(1, entry3.term());
        assertEquals(6, entry3.index());
        LogEntry entry4 = entries[3];
        assertEquals(1, entry4.term());
        assertEquals(7, entry4.index());
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(3, snapshotEntry.index());
        assertSingleRaftLogFile(3);
    }

    @Test
    public void when_multipleSnapshotsPersistedSequentially_then_lastSnapshotRestored() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));
        store.persistSnapshot(newSnapshotEntry(3));

        // When
        store.persistSnapshot(newSnapshotEntry(10));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(0, entries.length);
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(10, snapshotEntry.index());
        assertSingleRaftLogFile(10);
    }

    @Test
    public void when_termIsPersistedWithVotedFor_then_votedForRestored() throws IOException {
        // Given
        store.persistTerm(2, null);

        // When
        RaftEndpoint votedFor = newRaftMember(5010);
        store.persistTerm(2, votedFor);

        // Then
        RestoredRaftState restoredState = restoreState();
        assertEquals(2, restoredState.term());
        assertEquals(votedFor, restoredState.votedFor());
    }

    @Test
    public void when_initialMembersPersisted_then_initialMembersRestored() throws IOException {
        // initial members are persisted on init()

        // Then
        RestoredRaftState restoredState = restoreState();
        assertEquals(members, restoredState.initialMembers());
    }

    @Test
    public void when_storeClosed_then_persistedEntriesFlushed() throws IOException {
        // Given
        int entryCount = maxUncommittedEntries * 10;
        for (int i = 1; i <= entryCount; i++) {
            store.persistEntry(newLogEntry(i));
        }

        // When
        store.close();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(entryCount, entries.length);
        for (int i = 1; i <= entryCount; i++) {
            LogEntry entry = entries[i - 1];
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
            ApplyRaftRunnable runnable = (ApplyRaftRunnable) entry.operation();
            assertEquals("val" + i, runnable.getVal());
        }
    }

    @Test
    public void when_storeClosed_then_deletedEntriesFlushed() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));
        store.deleteEntriesFrom(3);

        // When
        store.close();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        LogEntry entry1 = entries[0];
        assertEquals(1, entry1.term());
        assertEquals(1, entry1.index());
        ApplyRaftRunnable runnable1 = (ApplyRaftRunnable) entry1.operation();
        assertEquals("val1", runnable1.getVal());
        LogEntry entry2 = entries[1];
        assertEquals(1, entry2.term());
        assertEquals(2, entry2.index());
        ApplyRaftRunnable runnable2 = (ApplyRaftRunnable) entry2.operation();
        assertEquals("val2", runnable2.getVal());
    }

    @Test
    public void when_storeClosed_then_persistedSnapshotFlushed() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));
        store.persistSnapshot(newSnapshotEntry(3));

        // When
        store.close();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(2, entries.length);
        LogEntry entry1 = entries[0];
        assertEquals(1, entry1.term());
        assertEquals(4, entry1.index());
        LogEntry entry2 = entries[1];
        assertEquals(1, entry2.term());
        assertEquals(5, entry2.index());
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(3, snapshotEntry.index());
    }

    @Test
    public void when_staleLogFilesExistOnRestore_then_mostRecentFileRestored() throws IOException {
        // Given
        int entryCount = maxUncommittedEntries * 10;
        for (int i = 1; i <= entryCount; i++) {
            store.persistEntry(newLogEntry(i));
        }
        store.flushLogs();

        File logFile = getRaftLogFile();
        File tempFile = copyToTempFile(logFile);

        long snapshotIndex = entryCount - 5;
        store.persistSnapshot(newSnapshotEntry(snapshotIndex));
        store.close();

        rename(tempFile, logFile);

        // When
        RestoredRaftState restoredState = restoreState();

        // Then
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(5, entries.length);
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(snapshotIndex, snapshotEntry.index());
        assertFalse(logFile.exists());
    }

    @Test
    public void when_storeRestored_then_newEntriesPersisted() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.close();

        OnDiskRaftStateLoader loader = getLoader();
        loader.load();
        store = openNewStore(loader.logFileStructure());

        // When
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(4, entries.length);
        for (int i = 1; i <= 4; i++) {
            LogEntry entry = entries[i - 1];
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void when_storeRestoredMultipleTimes_then_newEntriesPersisted() throws IOException {
        // When
        long logIndex = 0;
        int repeat = 10;
        int logEntryCountOnEachRound = maxUncommittedEntries;
        for (int round = 0; round < repeat; round++) {
            for (int i = 0; i < logEntryCountOnEachRound; i++) {
                logIndex++;
                store.persistEntry(newLogEntry(logIndex));
            }

            store.close();
            OnDiskRaftStateLoader loader = getLoader();
            loader.load();
            store = openNewStore(loader.logFileStructure());
        }

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(logIndex, entries.length);
        for (int i = 1; i <= logIndex; i++) {
            LogEntry entry = entries[i - 1];
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void when_storeRestoredMultipleTimes_then_newSnapshotsPersisted() throws IOException {
        // When
        long logIndex = 0;
        int repeat = 10;
        int logEntryCountOnEachRound = maxUncommittedEntries * 10;
        long snapshotIndex = 0;
        for (int round = 0; round < repeat; round++) {
            for (int i = 0; i < logEntryCountOnEachRound; i++) {
                logIndex++;
                store.persistEntry(newLogEntry(logIndex));
            }

            snapshotIndex = logIndex - (maxUncommittedEntries - 1);
            store.persistSnapshot(newSnapshotEntry(snapshotIndex));

            store.close();

            OnDiskRaftStateLoader loader = getLoader();
            loader.load();
            store = openNewStore(loader.logFileStructure());
        }

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(maxUncommittedEntries - 1, entries.length);
        for (int i = 0; i < entries.length; i++) {
            LogEntry entry = entries[i];
            assertEquals(1, entry.term());
            assertEquals(i + snapshotIndex + 1, entry.index());
        }
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(snapshotIndex, snapshotEntry.index());
    }

    @Test
    public void when_storeRestoredMultipleTimes_then_newEntriesPersistedAfterSnapshots() throws IOException {
        // When
        long logIndex = 0;
        int repeat = 10;
        int logEntryCountOnEachRound = maxUncommittedEntries * 10;
        long snapshotIndex = 0;
        for (int round = 0; round < repeat; round++) {
            for (int i = 0; i < logEntryCountOnEachRound; i++) {
                logIndex++;
                store.persistEntry(newLogEntry(logIndex));
            }

            snapshotIndex = logIndex - (maxUncommittedEntries - 1);
            store.persistSnapshot(newSnapshotEntry(snapshotIndex));

            for (int i = 0; i < logEntryCountOnEachRound; i++) {
                logIndex++;
                store.persistEntry(newLogEntry(logIndex));
            }

            store.close();

            OnDiskRaftStateLoader loader = getLoader();
            loader.load();
            store = openNewStore(loader.logFileStructure());
        }

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(logEntryCountOnEachRound + maxUncommittedEntries - 1, entries.length);
        for (int i = 0; i < entries.length; i++) {
            LogEntry entry = entries[i];
            assertEquals(1, entry.term());
            assertEquals(i + snapshotIndex + 1, entry.index());
        }
        SnapshotEntry snapshotEntry = restoredState.snapshot();
        assertNotNull(snapshotEntry);
        assertEquals(1, snapshotEntry.term());
        assertEquals(snapshotIndex, snapshotEntry.index());
    }

    @Test
    public void when_storeRestoredMultipleTimes_then_entriesDeletedAndNewEntriesPersisted() throws IOException {
        // When
        long logIndex = 0;
        int repeat = 10;
        int logEntryCountOnEachRound = maxUncommittedEntries;
        long deletedEntryCountOnEachRound = 5;
        for (int round = 0; round < repeat; round++) {
            for (int i = 0; i < logEntryCountOnEachRound; i++) {
                logIndex++;
                store.persistEntry(newLogEntry(logIndex));
            }

            store.close();
            OnDiskRaftStateLoader loader = getLoader();
            loader.load();
            store = openNewStore(loader.logFileStructure());

            logIndex -= deletedEntryCountOnEachRound;
            store.deleteEntriesFrom(logIndex + 1);
        }

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(logIndex, entries.length);
        for (int i = 1; i <= logIndex; i++) {
            LogEntry entry = entries[i - 1];
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void when_multipleSnapshotsPersistedWithoutFlushInBetween_then_noDanglingFileLeftAfterFlush() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistEntry(newLogEntry(2));
        store.persistEntry(newLogEntry(3));
        store.persistEntry(newLogEntry(4));
        store.persistEntry(newLogEntry(5));
        store.flushLogs();

        // When
        store.persistSnapshot(newSnapshotEntry(3));
        store.persistSnapshot(newSnapshotEntry(10));
        store.persistSnapshot(newSnapshotEntry(15));
        store.flushLogs();

        // Then
        assertSingleRaftLogFile(15);
    }

    @Test
    public void when_snapshotPersistedWithFutureIndex_then_newEntryPersistedAfterwards() throws IOException {
        // Given
        store.persistEntry(newLogEntry(1));
        store.persistSnapshot(newSnapshotEntry(3));

        // When
        store.persistEntry(newLogEntry(4));
        store.flushLogs();

        // Then
        RestoredRaftState restoredState = restoreState();
        LogEntry[] entries = restoredState.entries();
        assertNotNull(entries);
        assertEquals(1, entries.length);
        assertEquals(4, entries[0].index());

        SnapshotEntry snapshot = restoredState.snapshot();
        assertNotNull(snapshot);
        assertEquals(3, snapshot.index());
        assertSingleRaftLogFile(3);
    }

    private void assertSingleRaftLogFile(long logIndex) {
        String[] logFileNames = baseDir.list((dir, name) -> name.startsWith(RAFT_LOG_PREFIX));

        assertNotNull(logFileNames);
        assertEquals(1, logFileNames.length);
        assertEquals(getRaftLogFileName(logIndex), logFileNames[0]);
    }

    private LogEntry newLogEntry(long index) {
        return new LogEntry(1, index, new ApplyRaftRunnable("val" + index));
    }

    private SnapshotEntry newSnapshotEntry(long index) {
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), index, "snapshotAt" + index);
        return new SnapshotEntry(1, index, op, 0, members);
    }

    private File copyToTempFile(File logFile)
            throws IOException {
        File tempFile = new File(baseDir, logFile.getName() + ".tmp");
        boolean tempFileCreated = tempFile.createNewFile();
        assertTrue(tempFileCreated);
        copy(logFile, tempFile);
        return tempFile;
    }

    private RestoredRaftState restoreState() throws IOException {
        return getLoader().load();
    }

    private OnDiskRaftStateLoader getLoader() {
        return new OnDiskRaftStateLoader(baseDir, maxUncommittedEntries, serializationService);
    }

    private OnDiskRaftStateStore openNewStore(LogFileStructure logFileStructure) throws IOException {
        OnDiskRaftStateStore store = new OnDiskRaftStateStore(baseDir, serializationService, maxUncommittedEntries, logFileStructure);
        store.open();
        return store;
    }

    private File getRaftLogFile() {
        File[] logFiles = baseDir.listFiles((dir, name) -> name.startsWith(RAFT_LOG_PREFIX));

        assertNotNull(logFiles);
        assertEquals(1, logFiles.length);

        return logFiles[0];
    }

}
