package com.hazelcast.cp.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RestoreSnapshotRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftGroupId;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OnDiskRaftStateStoreTest {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule(true);

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    private int maxUncommittedEntries = 10;

    private File baseDir;

    private OnDiskRaftStateStore store;

    private List<RaftEndpoint> members;

    @Before
    public void init() throws IOException {
        baseDir = hotRestartFolderRule.getBaseDir();
        assertTrue(baseDir.exists() && baseDir.isDirectory());
        store = new OnDiskRaftStateStore(baseDir, serializationService, maxUncommittedEntries, null);
        store.open();

        store.persistTerm(1, null);
        TestRaftEndpoint endpoint1 = newRaftMember(5000);
        members = Arrays.<RaftEndpoint>asList(endpoint1, newRaftMember(5001), newRaftMember(5002));
        store.persistInitialMembers(endpoint1, members);
    }

    @Test
    public void when_singleEntryPersisted_then_entryLoaded() throws IOException {
        // When
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
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
        // When
        int entryCount = maxUncommittedEntries * 10;
        for (int i = 1; i <= entryCount; i++) {
            store.persistEntry(new LogEntry(1, i, new ApplyRaftRunnable("val" + i)));
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
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));

        // When-Then
        exceptionRule.expect(IllegalArgumentException.class);
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
    }

    @Test
    public void when_givenEntryIndexIsGreaterThanNextEntryIndex_then_fail() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));

        // When-Then
        exceptionRule.expect(IllegalArgumentException.class);
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val1")));
    }

    @Test
    public void when_multipleEntriesPersisted_then_entriesDeletedFromBottomIndex() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));

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
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));

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
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));

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
            store.persistEntry(new LogEntry(1, i, new ApplyRaftRunnable("val" + i)));
        }
        store.persistEntry(new LogEntry(1, maxUncommittedEntries + 1, new ApplyRaftRunnable("val" + (maxUncommittedEntries + 1))));

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        store.deleteEntriesFrom(1);
    }

    @Test
    public void when_notPersistedEntryDeleted_then_fail() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));

        // When-Then
        exceptionRule.expect(IndexOutOfBoundsException.class);
        store.deleteEntriesFrom(2);
    }

    @Test
    public void when_snapshotPersistedWithFurtherIndex_then_allPreviousEntriesDiscarded() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));

        // When
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 5, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 5, op, 0, members));
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
        assertEquals(members, new ArrayList<RaftEndpoint>(snapshotEntry.groupMembers()));
        RestoreSnapshotRaftRunnable restored = (RestoreSnapshotRaftRunnable) snapshotEntry.operation();
        assertEquals("snapshot", restored.getSnapshot());
    }

    @Test
    public void when_snapshotPersistedWithTopIndex_then_allPreviousEntriesDiscarded() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));

        // When
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 5, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 5, op, 0, members));
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
        assertEquals(members, new ArrayList<RaftEndpoint>(snapshotEntry.groupMembers()));
        RestoreSnapshotRaftRunnable restored = (RestoreSnapshotRaftRunnable) snapshotEntry.operation();

        assertEquals("snapshot", restored.getSnapshot());
    }

    @Test
    public void when_snapshotPersistedWithMiddleIndex_then_allPreviousEntriesDiscarded() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));

        // When
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 3L, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 3, op, 0, members));
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
    }

    @Test
    public void when_snapshotPersistedWithBottomIndex_then_onlyBottomEntryDiscarded() throws IOException {
        // Given
        for (int i = 1; i <= maxUncommittedEntries; i++) {
            store.persistEntry(new LogEntry(1, i, new ApplyRaftRunnable("val" + i)));
        }
        store.persistEntry(new LogEntry(1, maxUncommittedEntries + 1, new ApplyRaftRunnable("val" + (maxUncommittedEntries + 1))));

        // When
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 2L, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 2, op, 0, members));
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
    }

    @Test
    public void when_snapshotPersistedWithInvalidIndex_then_fail() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));

        // When-Then
        exceptionRule.expect(IllegalArgumentException.class);
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 1L, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 1, op, 0, members));

    }

    @Test
    public void when_newEntriesPersistedAfterSnapshot_then_newEntriesLoaded() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 3L, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 3, op, 0, members));

        // When
        store.persistEntry(new LogEntry(1, 6, new ApplyRaftRunnable("val6")));
        store.persistEntry(new LogEntry(1, 7, new ApplyRaftRunnable("val7")));
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
    }

    @Test
    public void when_multipleSnapshotsPersistedSequentially_then_lastSnapshotRestored() throws IOException {
        // Given
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));
        Object snapshotOp1 = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 3L, "snapshot1");
        store.persistSnapshot(new SnapshotEntry(1, 3, snapshotOp1, 0, members));

        // When
        Object snapshotOp2 = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 10L, "snapshot2");
        store.persistSnapshot(new SnapshotEntry(1, 10, snapshotOp2, 0, members));
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
            store.persistEntry(new LogEntry(1, i, new ApplyRaftRunnable("val" + i)));
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
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));
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
        store.persistEntry(new LogEntry(1, 1, new ApplyRaftRunnable("val1")));
        store.persistEntry(new LogEntry(1, 2, new ApplyRaftRunnable("val2")));
        store.persistEntry(new LogEntry(1, 3, new ApplyRaftRunnable("val3")));
        store.persistEntry(new LogEntry(1, 4, new ApplyRaftRunnable("val4")));
        store.persistEntry(new LogEntry(1, 5, new ApplyRaftRunnable("val5")));
        Object op = new RestoreSnapshotRaftRunnable(new TestRaftGroupId("default"), 3L, "snapshot");
        store.persistSnapshot(new SnapshotEntry(1, 3, op, 0, members));

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

    private RestoredRaftState restoreState() throws IOException {
        return new OnDiskRaftStateLoader(baseDir, maxUncommittedEntries, serializationService).load();
    }

}
