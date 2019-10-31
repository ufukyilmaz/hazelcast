package com.hazelcast.cp.persistence;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.dataservice.RestoreSnapshotRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateLoader;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntFunction;

import static com.hazelcast.config.cp.RaftAlgorithmConfig.DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;
import static com.hazelcast.cp.internal.raft.MembershipChangeMode.REMOVE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastApplied;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftPersistenceTest extends HazelcastTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    private final BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateLoader> stateLoaderFactory =
            (endpoint, config) -> getStateLoader(endpoint, config.getUncommittedEntryCountToRejectNewAppends());

    private final BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore> stateStoreFactory =
            (endpoint, config) -> {
                OnDiskRaftStateLoader loader = (OnDiskRaftStateLoader) stateLoaderFactory.apply(endpoint, config);
                try {
                    loader.load();
                    return new OnDiskRaftStateStore(getDirectory(endpoint), serializationService,
                            config.getUncommittedEntryCountToRejectNewAppends(), loader.logFileStructure());
                } catch (Exception e) {
                    return new OnDiskRaftStateStore(getDirectory(endpoint), serializationService,
                            config.getUncommittedEntryCountToRejectNewAppends(), null);
                }
            };

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testTermAndVoteArePersisted() {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        int term1 = getTerm(leader);
        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertEquals(term1, getTerm(node));
                OnDiskRaftStateLoader loader = getStateLoader(node.getLocalMember());
                RestoredRaftState restoredState = loader.load();
                assertEquals(term1, restoredState.term());
                assertEquals(new ArrayList<>(node.getInitialMembers()), new ArrayList<>(restoredState.initialMembers()));
            }
        });

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint l = node.getLeader();
                assertNotNull(l);
                assertNotEquals(leader.getLeader(), l);
            }
        });

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        int term2 = getTerm(newLeader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : followers) {
                assertEquals(term2, getTerm(node));
                OnDiskRaftStateLoader loader = getStateLoader(node.getLocalMember());
                RestoredRaftState restoredState = loader.load();
                assertEquals(term2, restoredState.term());
                assertEquals(newLeader.getLocalMember(), restoredState.votedFor());
            }
        });
    }

    @Test
    public void testCommittedEntriesArePersisted() throws Exception {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalMember())) {
                assertEquals(getCommitIndex(leader), getCommitIndex(follower));
            }
        });

        ensureFlush(group.getNodes());
        group.destroy();

        for (RaftNodeImpl node : group.getNodes()) {
            OnDiskRaftStateLoader loader = getStateLoader(node.getLocalMember());
            RestoredRaftState restoredState = loader.load();
            LogEntry[] entries = restoredState.entries();
            assertEquals(count, entries.length);
            for (int i = 0; i < count; i++) {
                LogEntry entry = entries[i];
                assertEquals(i + 1, entry.index());
                assertEquals("val" + i, ((ApplyRaftRunnable) entry.operation()).getVal());
            }
        }
    }

    @Test
    public void testUncommittedEntriesArePersisted() throws IOException {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl responsiveFollower = followers[0];

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalMember(), followers[i].getLocalMember(), AppendRequest.class);
        }

        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i));
        }

        assertTrueEventually(() -> assertEquals(getLastLogOrSnapshotEntry(leader).index(),
                getLastLogOrSnapshotEntry(responsiveFollower).index()));

        ensureFlush(group.getNodes());
        group.destroy();

        for (RaftNodeImpl node : asList(leader, responsiveFollower)) {
            OnDiskRaftStateLoader loader = getStateLoader(node.getLocalMember());
            RestoredRaftState restoredState = loader.load();
            LogEntry[] entries = restoredState.entries();
            assertEquals(count, entries.length);
            for (int i = 0; i < count; i++) {
                LogEntry entry = entries[i];
                assertEquals(i + 1, entry.index());
                assertEquals("val" + i, ((ApplyRaftRunnable) entry.operation()).getVal());
            }
        }
    }

    @Test
    public void testSnapshotIsPersisted() throws Exception {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertEquals(committedEntryCountToSnapshot, getSnapshotEntry(node).index());
            }
        });

        ensureFlush(group.getNodes());
        group.destroy();

        for (RaftNodeImpl node : group.getNodes()) {
            OnDiskRaftStateLoader loader = getStateLoader(node.getLocalMember());
            RestoredRaftState restoredState = loader.load();
            SnapshotEntry snapshot = restoredState.snapshot();
            assertNotNull(snapshot);
            assertEquals(committedEntryCountToSnapshot, snapshot.index());
            RestoreSnapshotRaftRunnable runnable = (RestoreSnapshotRaftRunnable) snapshot.operation();
            assertEquals(committedEntryCountToSnapshot, runnable.getCommitIndex());
            Map<Long, Object> snapshotState = (Map<Long, Object>) runnable.getSnapshot();
            for (int i = 0; i < committedEntryCountToSnapshot; i++) {
                long key = i + 1;
                assertEquals("i: " + i, "val" + i, snapshotState.get(key));
            }
        }
    }


    @Test
    public void when_leaderAppendEntriesInMinoritySplit_then_itTruncatesEntriesOnStore() throws Exception {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val1")).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(1, getCommitIndex(raftNode));
            }
        });

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalMember(), leaderEndpoint);
            }
        });

        for (int i = 0; i < 5; i++) {
            leader.replicate(new ApplyRaftRunnable("isolated" + i));
        }

        assertTrueEventually(() -> assertEquals(6, getLastLogOrSnapshotEntry(leader).index()));

        ensureFlush(leader);

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(new ApplyRaftRunnable("valNew" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(11, getCommitIndex(raftNode));
            }
        });

        group.merge();

        RaftNodeImpl finalLeader = group.waitUntilLeaderElected();

        assertNotEquals(leader.getLocalMember(), finalLeader.getLocalMember());

        assertTrueEventually(() -> assertEquals(11, getCommitIndex(leader)));

        ensureFlush(leader);

        OnDiskRaftStateLoader loader = getStateLoader(leader.getLocalMember());
        RestoredRaftState restoredState = loader.load();
        LogEntry[] entries = restoredState.entries();
        assertEquals(11, entries.length);
        assertEquals("val1", ((ApplyRaftRunnable) entries[0].operation()).getVal());
        for (int i = 1; i < 11; i++) {
            assertEquals("valNew" + (i - 1), ((ApplyRaftRunnable) entries[i].operation()).getVal());
        }
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftState() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig();
        group = new LocalRaftGroupBuilder(3, config).setRaftStateStoreFactory(stateStoreFactory)
                                            .setAppendNopEntryOnLeaderElection(true).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(terminatedEndpoint);
        RestoredRaftState terminatedState = loader.load();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(newLeader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(newLeader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(newLeader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(newLeader), getTerm(restartedNode));
            assertEquals(getCommitIndex(newLeader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(newLeader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(count));
            for (int i = 0; i < count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itRestoresItsRaftStateAndBecomesLeader() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setRaftStateStoreFactory(stateStoreFactory)
                                                    .setAppendNopEntryOnLeaderElection(true)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        int term = getTerm(leader);
        long commitIndex = getCommitIndex(leader);

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(newLeader, restartedNode);

        assertTrueEventually(() -> {
            assertTrue(getTerm(restartedNode) > term);
            assertEquals(commitIndex + 1, getCommitIndex(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(count));
            for (int i = 0; i < count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_followerIsRestarted_then_itRestoresItsRaftState() throws Exception {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl terminatedFollower = group.getAnyFollowerNode();
        int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertEquals(getCommitIndex(leader), getCommitIndex(terminatedFollower)));

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        ensureFlush(terminatedFollower);
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(terminatedEndpoint);
        RestoredRaftState terminatedState = loader.load();

        leader.replicate(new ApplyRaftRunnable("val" + count)).get();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(leader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(leader), getTerm(restartedNode));
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(count + 1));
            for (int i = 0; i <= count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftStateWithSnapshot() throws Exception {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrue(getSnapshotEntry(leader).index() > 0);

        ensureFlush(leader);
        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(newLeader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(newLeader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(newLeader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(newLeader), getTerm(restartedNode));
            assertEquals(getCommitIndex(newLeader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(newLeader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(committedEntryCountToSnapshot + 1));
            for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_followerIsRestarted_then_itRestoresItsRaftStateWithSnapshot() throws Exception {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertTrue(getSnapshotEntry(node).index() > 0);
            }
        });

        RaftNodeImpl terminatedFollower = group.getAnyFollowerNode();
        ensureFlush(terminatedFollower);
        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(terminatedEndpoint);
        RestoredRaftState terminatedState = loader.load();

        leader.replicate(new ApplyRaftRunnable("val" + (committedEntryCountToSnapshot + 1))).get();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(leader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(leader), getTerm(restartedNode));
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(committedEntryCountToSnapshot + 2));
            for (int i = 0; i <= committedEntryCountToSnapshot + 1; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itRestoresItsRaftStateWithSnapshotAndBecomesLeader() throws Exception {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot)
                .setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrue(getSnapshotEntry(leader).index() > 0);
        int term = getTerm(leader);
        long commitIndex = getCommitIndex(leader);

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        assertTrueEventually(() -> {
            assertTrue(getTerm(newLeader) > term);
            assertEquals(commitIndex + 1, getCommitIndex(newLeader));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(committedEntryCountToSnapshot + 1));
            for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberList() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl runningFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(runningFollower), getCommitIndex(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(runningFollower).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(runningFollower).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberList() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl terminatedFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        ensureFlush(terminatedFollower);
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberListViaSnapshot() throws Exception {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot)
                .setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl runningFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        while (getSnapshotEntry(leader).index() == 0) {
            leader.replicate(new ApplyRaftRunnable("val")).get();
        }

        ensureFlush(leader);

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(runningFollower), getCommitIndex(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(runningFollower).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(runningFollower).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberListViaSnapshot() throws Exception {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot)
                .setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(stateStoreFactory)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl terminatedFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        while (getSnapshotEntry(terminatedFollower).index() == 0) {
            leader.replicate(new ApplyRaftRunnable("val")).get();
        }

        ensureFlush(terminatedFollower);

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        group.terminateNode(terminatedEndpoint);
        OnDiskRaftStateLoader loader = getStateLoader(
                terminatedEndpoint, config.getUncommittedEntryCountToRejectNewAppends());
        RestoredRaftState terminatedState = loader.load();

        RaftStateStore stateStore = createStateStore(terminatedEndpoint, loader);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void test_committedEntriesSurviveWholeGroupCrash() throws Exception {
        int committedEntryCountToSnapshot = 50;
        int committedEntryCountOnEachRound = 75;
        int repeat = 25;
        long entryIndex = 0;

        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config)
                .setAppendNopEntryOnLeaderElection(true)
                .setRaftStateStoreFactory(stateStoreFactory).build();
        group.start();

        Map<Integer, TestRaftEndpoint> endpointMap = new HashMap<>();
        for (RaftNodeImpl node : group.getNodes()) {
            TestRaftEndpoint endpoint = (TestRaftEndpoint) node.getLocalMember();
            endpointMap.put(endpoint.getPort(), endpoint);
        }

        IntFunction<TestRaftEndpoint> endpointFactory = endpointMap::get;

        for (int round = 0; round < repeat; round++) {
            RaftNodeImpl leader = group.waitUntilLeaderElected();

            for (int i = 0; i < committedEntryCountOnEachRound; i++) {
                entryIndex++;
                String value = "val" + entryIndex;
                leader.replicate(new ApplyRaftRunnable(value)).get();
            }

            group.destroy();

            group = new LocalRaftGroupBuilder(3, config)
                    .setAppendNopEntryOnLeaderElection(true)
                    .setEndpointFactory(endpointFactory)
                    .setRaftStateStoreFactory(stateStoreFactory)
                    .setRaftStateLoaderFactory(stateLoaderFactory).build();
            group.start();
        }

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        long finalEntryIndex = entryIndex;
        assertTrueEventually(() -> {
            RaftDataService service = group.getService(leader.getLocalMember());
            Set<Object> committedValues = service.values();
            for (int i = 1; i <= finalEntryIndex; i++) {
                String value = "val" + i;
                assertTrue(value + " does not exist!", committedValues.contains(value));
            }
        });
    }

    private RaftStateStore createStateStore(RaftEndpoint endpoint, OnDiskRaftStateLoader loader) {
        return new OnDiskRaftStateStore(getDirectory(endpoint), serializationService, loader.maxUncommittedEntries(),
                loader.logFileStructure());
    }

    private OnDiskRaftStateLoader getStateLoader(RaftEndpoint endpoint) {
        return getStateLoader(endpoint, DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS);
    }

    private OnDiskRaftStateLoader getStateLoader(RaftEndpoint endpoint, int maxUncommittedEntryCount) {
        return new OnDiskRaftStateLoader(getDirectory(endpoint), maxUncommittedEntryCount, serializationService);
    }

    private File getDirectory(RaftEndpoint endpoint) {
        File dir = new File(hotRestartFolderRule.getBaseDir(), endpoint.getUuid().toString());
        dir.mkdirs();
        checkState(dir.exists(), dir + " does not exist!");
        return dir;
    }

    private void ensureFlush(RaftNodeImpl... nodes) {
        CountDownLatch latch = new CountDownLatch(nodes.length);
        for (RaftNodeImpl node : nodes) {
            node.execute(() -> {
                node.state().log().flush();
                latch.countDown();
            });
        }

        assertOpenEventually(latch);
    }

    private void blockVotingBetweenFollowers() {
        RaftEndpoint[] endpoints = group.getFollowerEndpoints();
        for (RaftEndpoint endpoint : endpoints) {
            if (group.isRunning(endpoint)) {
                group.dropMessagesToAll(endpoint, PreVoteRequest.class);
            }
        }
    }

}
