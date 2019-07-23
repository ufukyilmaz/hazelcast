package com.hazelcast.cp.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftTestApplyOp;
import com.hazelcast.cp.internal.RaftTestQueryOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterPersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestartedConcurrently_then_membersRestoreAndTermIncreases() {
        when_wholeClusterRestarted_then_membersRestoreAndTermIncreases(true);
    }

    @Test
    public void when_wholeClusterRestarted_then_membersRestoreAndTermIncreases() {
        when_wholeClusterRestarted_then_membersRestoreAndTermIncreases(false);
    }

    private void when_wholeClusterRestarted_then_membersRestoreAndTermIncreases(boolean parallelRestart) {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);

        Address[] addresses = getAddresses(instances);
        waitUntilCPDiscoveryCompleted(instances);

        Map<Address, CPMember> cpMemberMap = new HashMap<Address, CPMember>();
        for (HazelcastInstance instance : instances) {
            cpMemberMap.put(getAddress(instance), instance.getCPSubsystem().getLocalCPMember());
        }

        int term = awaitLeaderElectionAndGetTerm(instances);
        for (int i = 0; i < 3; i++) {
            term = restartAndAssertMembers(config, addresses, cpMemberMap, term, parallelRestart);
        }
    }

    private int restartAndAssertMembers(Config config, Address[] addresses, Map<Address, CPMember> cpMembers,
            int term, boolean parallelRestart) {
        factory.terminateAll();

        HazelcastInstance[] instances = parallelRestart
                ? restartInstancesParallel(addresses, config)
                : restartInstances(addresses, config);

        waitUntilCPDiscoveryCompleted(instances);

        for (HazelcastInstance instance : instances) {
            CPMember member = cpMembers.get(getAddress(instance));
            assertNotNull(member);
            assertEquals(member, instance.getCPSubsystem().getLocalCPMember());
        }

        int newTerm = awaitLeaderElectionAndGetTerm(instances);
        assertThat(newTerm, greaterThan(term));
        return newTerm;
    }

    private int awaitLeaderElectionAndGetTerm(HazelcastInstance[] instances) {
        RaftGroupId metadataGroupId = getMetadataGroupId(instances[0]);
        waitAllForLeaderElection(instances, metadataGroupId);
        RaftNodeImpl raftNode = getRaftNode(instances[0], metadataGroupId);
        return getTerm(raftNode);
    }

    @Test
    public void when_wholeClusterRestarted_withMultipleGroups_then_groupsAreRestored() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        for (int i = 0; i < 3; i++) {
            RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
            RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
            RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();
            RaftGroupId group3 = invocationManager.createRaftGroup("group3").join();

            factory.terminateAll();

            instances = restartInstancesParallel(addresses, config);
            final RaftService raftService = getRaftService(instances[0]);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertThat(raftService.getCPGroupIds().get(), hasSize(4));
                }
            });

            Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
            assertThat(groupIds, hasItem(group1));
            assertThat(groupIds, hasItem(group2));
            assertThat(groupIds, hasItem(group3));

            assertEquals(group1, raftService.getCPGroup(group1.name()).get().id());
            assertEquals(group2, raftService.getCPGroup(group2.name()).get().id());
            assertEquals(group3, raftService.getCPGroup(group3.name()).get().id());
        }
    }

    @Test
    public void when_MemberShutdown_then_notRemovedFromCpGroups() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        final RaftGroupId groupId = getMetadataGroupId(instances[0]);
        HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
        final HazelcastInstance followerInstance = getRandomFollowerInstance(instances, groupId);

        final RaftNodeImpl node = getRaftNode(followerInstance, groupId);
        final RaftEndpoint leader = node.getLeader();
        CPMember leaderMember = leaderInstance.getCPSubsystem().getLocalCPMember();
        leaderInstance.shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotEquals(leader, node.getLeader());
            }
        });

        Collection<CPMember> members = followerInstance.getCPSubsystem()
                .getCPSubsystemManagementService().getCPMembers().get();
        assertEquals(3, members.size());
        assertThat(members, hasItem(leaderMember));
    }

    @Test
    public void when_CpSubsystemReset_then_startsEmpty() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
        RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();
        RaftGroupId group3 = invocationManager.createRaftGroup("group3").join();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().restart().get();
        long seed = getMetadataGroupId(instances[0]).seed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstancesParallel(addresses, config);
        RaftService raftService = getRaftService(instances[0]);

        Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
        assertThat(groupIds, hasSize(1));

        assertNull(raftService.getCPGroup(group1.name()).get());
        assertNull(raftService.getCPGroup(group2.name()).get());
        assertNull(raftService.getCPGroup(group3.name()).get());

        assertEquals(seed, getMetadataGroupId(instances[0]).seed());
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        final HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);
        final RaftGroupId group = invocationManager.createRaftGroup("group").join();

        invocationManager.invoke(group, new RaftTestApplyOp(1)).join();

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        final long newValue = 2;
        final Future<Long> f = spawn(new Callable<Long>() {
            @Override
            public Long call() {
                return invocationManager.<Long>invoke(group, new RaftTestApplyOp(newValue)).join();
            }
        });

        // Invocation cannot complete without majority
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse(f.isDone());
            }
        }, 3);

        // restart majority back
        instances[0] = factory.newHazelcastInstance(addresses[0], config);
        instances[1] = factory.newHazelcastInstance(addresses[1], config);

        long value = f.get();
        assertEquals(newValue, value);

        assertCommitIndexesSame(instances, group);
    }

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);
        final RaftGroupId group = invocationManager.createRaftGroup("group").join();

        final int increments = 5000;
        final Future<Integer> f = spawn(new Callable<Integer>() {
            @Override
            public Integer call() {
                for (int i = 0; i < increments; i++) {
                    invocationManager.invoke(group, new RaftTestApplyOp(i + 1)).join();
                    sleepMillis(1);
                }
                return invocationManager.<Integer>query(group, new RaftTestQueryOp(), QueryPolicy.LINEARIZABLE).join();
            }
        });

        sleepSeconds(1);
        // crash majority
        instances[0].getLifecycleService().terminate();

        sleepSeconds(1);
        instances[1].getLifecycleService().terminate();

        // restart majority back
        instances[1] = factory.newHazelcastInstance(addresses[1], config);
        sleepSeconds(1);
        instances[0] = factory.newHazelcastInstance(addresses[0], config);

        int value = f.get();
        assertEquals(value, increments);

        assertCommitIndexesSame(instances, group);
    }
}
