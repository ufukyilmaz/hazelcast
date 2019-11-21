package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftTestApplyOp;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterPersistenceTest extends PersistenceTestSupport {

    @Test
    public void when_wholeClusterRestarted_then_membersRestoreAndTermIncreases() {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);

        Address[] addresses = getAddresses(instances);
        waitUntilCPDiscoveryCompleted(instances);

        Set<CPMember> cpMembers = new HashSet<>();
        for (HazelcastInstance instance : instances) {
            cpMembers.add(instance.getCPSubsystem().getLocalCPMember());
        }

        int term = awaitLeaderElectionAndGetTerm(instances, getMetadataGroupId(instances[0]));
        for (int i = 0; i < 3; i++) {
            term = restartAndAssertMembers(config, addresses, cpMembers, term);
        }
    }

    private int restartAndAssertMembers(Config config, Address[] addresses, Set<CPMember> cpMembers, int term) {
        factory.terminateAll();

        HazelcastInstance[] instances = restartInstances(addresses, config);

        waitUntilCPDiscoveryCompleted(instances);

        Set<UUID> uuids = new HashSet<>();
        for (CPMember cpMember : cpMembers) {
            uuids.add(cpMember.getUuid());
        }

        for (HazelcastInstance instance : instances) {
            CPMember localCPMember = instance.getCPSubsystem().getLocalCPMember();
            assertThat(uuids, hasItem(localCPMember.getUuid()));
            uuids.remove(localCPMember.getUuid());
        }
        assertThat(uuids, empty());

        int newTerm = awaitLeaderElectionAndGetTerm(instances, getMetadataGroupId(instances[0]));
        assertThat(newTerm, greaterThan(term));
        return newTerm;
    }

    static int awaitLeaderElectionAndGetTerm(HazelcastInstance[] instances, RaftGroupId groupId) {
        waitAllForLeaderElection(instances, groupId);
        RaftNodeImpl raftNode = getRaftNode(instances[0], groupId);
        return getTerm(raftNode);
    }

    @Test
    public void when_wholeClusterRestarted_withMultipleGroups_then_groupsAreRestored() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);
        RaftGroupId metadataGroupId = getMetadataGroupId(instances[0]);

        for (int i = 0; i < 3; i++) {
            RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
            RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
            RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();

            Map<RaftGroupId, Integer> terms = new HashMap<>();
            terms.put(metadataGroupId, awaitLeaderElectionAndGetTerm(instances, metadataGroupId));
            terms.put(group1, awaitLeaderElectionAndGetTerm(instances, group1));
            terms.put(group2, awaitLeaderElectionAndGetTerm(instances, group2));

            factory.terminateAll();

            instances = restartInstances(addresses, config);
            RaftService raftService = getRaftService(instances[0]);

            assertTrueEventually(() -> assertThat(raftService.getCPGroupIds().get(), hasSize(3)));

            Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
            assertThat(groupIds, hasItem(group1));
            assertThat(groupIds, hasItem(group2));

            assertEquals(group1, raftService.getCPGroup(group1.getName()).get().id());
            assertEquals(group2, raftService.getCPGroup(group2.getName()).get().id());

            int newTerm0 = awaitLeaderElectionAndGetTerm(instances, metadataGroupId);
            int newTerm1 = awaitLeaderElectionAndGetTerm(instances, group1);
            int newTerm2 = awaitLeaderElectionAndGetTerm(instances, group2);
            assertThat(newTerm0, greaterThan(terms.get(metadataGroupId)));
            assertThat(newTerm1, greaterThan(terms.get(group1)));
            assertThat(newTerm2, greaterThan(terms.get(group2)));
        }
    }

    @Test
    public void when_memberShutdown_then_notRemovedFromCPGroups() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        RaftGroupId groupId = getMetadataGroupId(instances[0]);
        HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
        HazelcastInstance followerInstance = getRandomFollowerInstance(instances, groupId);

        RaftNodeImpl node = getRaftNode(followerInstance, groupId);
        RaftEndpoint leader = node.getLeader();
        CPMember leaderMember = leaderInstance.getCPSubsystem().getLocalCPMember();
        leaderInstance.shutdown();

        assertTrueEventually(() -> assertNotEquals(leader, node.getLeader()));

        Collection<CPMember> members = followerInstance.getCPSubsystem()
                .getCPSubsystemManagementService().getCPMembers()
                .toCompletableFuture().get();
        assertEquals(3, members.size());
        assertThat(members, hasItem(leaderMember));
    }

    @Test
    public void when_memberRestarts_then_restoresData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);
        RaftGroupId group = invocationManager.createRaftGroup("group").join();

        invocationManager.invoke(group, new RaftTestApplyOp(1)).join();

        // shutdown majority
        instances[0].shutdown();
        instances[1].shutdown();

        long newValue = 2;
        Future<Long> f = spawn(() -> invocationManager.<Long>invoke(group, new RaftTestApplyOp(newValue)).join());

        // Invocation cannot complete without majority
        assertTrueAllTheTime(() -> assertFalse(f.isDone()), 3);

        // restart majority back
        instances[0] = restartInstance(addresses[0], config);
        instances[1] = restartInstance(addresses[1], config);

        long value = f.get();
        assertEquals(newValue, value);

        assertNotNull(instances[0].getCPSubsystem().getLocalCPMember());
        assertNotNull(instances[1].getCPSubsystem().getLocalCPMember());
        assertCommitIndexesSame(instances, group);
    }

    @Test
    public void when_memberRestartsWithNewAddress_then_oldMemberOfItsAddressNotAutomaticallyRemoved() throws Exception {
        assumeThat(restartAddressPolicy, equalTo(AddressPolicy.REUSE_RANDOM));

        Config config = createConfig(3, 3);
        config.getCPSubsystemConfig().setSessionTimeToLiveSeconds(20).setMissingCPMemberAutoRemovalSeconds(20);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        for (HazelcastInstance instance : instances) {
            instance.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(120, SECONDS);
        }

        CPMember cpMember0 = instances[0].getCPSubsystem().getLocalCPMember();
        CPMember cpMember1 = instances[1].getCPSubsystem().getLocalCPMember();

        sleepSeconds(1);
        instances[0].getLifecycleService().terminate();

        sleepSeconds(1);
        instances[1].getLifecycleService().terminate();

        for (File persistenceDirectory : baseDir.listFiles()) {
            if (persistenceDirectory.isFile()) {
                continue;
            }

            CPMemberInfo cpMember = new CPMetadataStoreImpl(persistenceDirectory, getSerializationService(instances[2])).readLocalCPMember();
            if (cpMember0.equals(cpMember)) {
                delete(persistenceDirectory);
                break;
            }
        }

        HazelcastInstance restartedInstance = restartInstance(addresses[0], config);

        assertTrueEventually(() -> assertNotNull(restartedInstance.getCPSubsystem().getLocalCPMember()));

        assertTrueAllTheTime(() -> {
            Collection<CPMember> cpMembers = instances[2].getCPSubsystem()
                                                         .getCPSubsystemManagementService()
                                                         .getCPMembers()
                                                         .toCompletableFuture()
                                                             .get();

            assertTrue(cpMembers.contains(restartedInstance.getCPSubsystem().getLocalCPMember()));
            assertTrue(cpMembers.contains(cpMember0));
            assertFalse(cpMembers.contains(cpMember1));
        }, 20);
    }

    @Test
    public void when_membersRollingRestarted_then_recoverDataAndMakeProgress() {
        Config config = createConfig(5, 5);
        HazelcastInstance[] instances = factory.newInstances(config, 5);
        waitUntilCPDiscoveryCompleted(instances);

        HazelcastInstance proxyInstance = factory.newHazelcastInstance(config);
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        List<RaftGroupId> groups = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            for (int k = 0; k < instances.length; k++) {
                Address address = getAddress(instances[k]);
                instances[k].getLifecycleService().terminate();

                int val = i + k;
                RaftGroupId groupId = invocationManager.createRaftGroup("group-" + val).joinInternal();
                groups.add(groupId);

                instances[k] = restartInstance(address, config);
                waitAllForLeaderElection(instances, groupId);
            }
        }

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftService raftService = getRaftService(instance);
                for (RaftGroupId groupId : groups) {
                    RaftNode raftNode = raftService.getRaftNode(groupId);
                    assertNotNull(raftNode);
                    assertEquals(groupId, raftNode.getGroupId());
                }
            }
        });
    }

    @Test
    public void when_memberRestarted_afterItIsRemovedFromCP_then_itShouldFail() throws Exception {
        Config config = createConfig(5, 5);
        HazelcastInstance[] instances = factory.newInstances(config, 5);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(4, instances[1]);
        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(cpMember.getUuid())
                    .toCompletableFuture().get();

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart!");
        } catch (IllegalStateException ignored) {
        }
    }
}
