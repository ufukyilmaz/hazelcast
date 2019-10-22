package com.hazelcast.cp.persistence;

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
import com.hazelcast.cp.internal.RaftTestQueryOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.DirectoryLock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

    private int awaitLeaderElectionAndGetTerm(HazelcastInstance[] instances, RaftGroupId groupId) {
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
            RaftGroupId group3 = invocationManager.createRaftGroup("group3").join();

            Map<RaftGroupId, Integer> terms = new HashMap<>();
            terms.put(metadataGroupId, awaitLeaderElectionAndGetTerm(instances, metadataGroupId));
            terms.put(group1, awaitLeaderElectionAndGetTerm(instances, group1));
            terms.put(group2, awaitLeaderElectionAndGetTerm(instances, group2));
            terms.put(group3, awaitLeaderElectionAndGetTerm(instances, group3));

            factory.terminateAll();

            instances = restartInstances(addresses, config);
            RaftService raftService = getRaftService(instances[0]);

            assertTrueEventually(() -> assertThat(raftService.getCPGroupIds().get(), hasSize(4)));

            Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
            assertThat(groupIds, hasItem(group1));
            assertThat(groupIds, hasItem(group2));
            assertThat(groupIds, hasItem(group3));

            assertEquals(group1, raftService.getCPGroup(group1.getName()).get().id());
            assertEquals(group2, raftService.getCPGroup(group2.getName()).get().id());
            assertEquals(group3, raftService.getCPGroup(group3.getName()).get().id());

            int newTerm0 = awaitLeaderElectionAndGetTerm(instances, metadataGroupId);
            int newTerm1 = awaitLeaderElectionAndGetTerm(instances, group1);
            int newTerm2 = awaitLeaderElectionAndGetTerm(instances, group2);
            int newTerm3 = awaitLeaderElectionAndGetTerm(instances, group3);
            assertThat(newTerm0, greaterThan(terms.get(metadataGroupId)));
            assertThat(newTerm1, greaterThan(terms.get(group1)));
            assertThat(newTerm2, greaterThan(terms.get(group2)));
            assertThat(newTerm3, greaterThan(terms.get(group3)));
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
    public void when_cpSubsystemReset_then_startsEmpty() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
        RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();
        RaftGroupId group3 = invocationManager.createRaftGroup("group3").join();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();
        long seed = getMetadataGroupId(instances[0]).getSeed();
        waitUntilCPDiscoveryCompleted(instances);

        factory.terminateAll();

        instances = restartInstances(addresses, config);
        RaftService raftService = getRaftService(instances[0]);

        Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
        assertThat(groupIds, hasSize(1));

        assertNull(raftService.getCPGroup(group1.getName()).get());
        assertNull(raftService.getCPGroup(group2.getName()).get());
        assertNull(raftService.getCPGroup(group3.getName()).get());

        assertEquals(seed, getMetadataGroupId(instances[0]).getSeed());
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
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);
        RaftGroupId group = invocationManager.createRaftGroup("group").join();

        int increments = 5000;
        Future<Integer> f = spawn(() -> {
            for (int i = 0; i < increments; i++) {
                invocationManager.invoke(group, new RaftTestApplyOp(i + 1)).join();
                sleepMillis(1);
            }
            return invocationManager.<Integer>query(group, new RaftTestQueryOp(), QueryPolicy.LINEARIZABLE).join();
        });

        sleepSeconds(1);
        // crash majority
        instances[0].getLifecycleService().terminate();

        sleepSeconds(1);
        instances[1].getLifecycleService().terminate();

        // restart majority back
        instances[1] = restartInstance(addresses[1], config);
        sleepSeconds(1);
        instances[0] = restartInstance(addresses[0], config);

        int value = f.get();
        assertEquals(value, increments);

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

            CPMemberInfo cpMember = new CPMetadataStoreImpl(persistenceDirectory).readLocalCPMember();
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
    public void when_cpSubsystemIsResetWhileCPMemberIsDown_then_cpMemberCannotRestart() throws Exception {
        Config config = createConfig(3, 3);
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "30000");

        HazelcastInstance[] instances = factory.newInstances(config, 4);
        assertClusterSizeEventually(4, instances);

        for (HazelcastInstance instance : Arrays.asList(instances).subList(0, 3)) {
            instance.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(120, SECONDS);
        }

        Address restartingAddress = getAddress(instances[0]);
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(3, instances[1]);
        instances[1].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        for (HazelcastInstance instance : Arrays.asList(instances[1], instances[2], instances[3])) {
            instance.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(120, SECONDS);
        }

        try {
            restartInstance(restartingAddress, config);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_groupDestroyed_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(false, false);
    }

    @Test
    public void when_groupDestroyed_afterLeaderElection_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(false, true);
    }

    @Test
    public void when_groupForceDestroyed_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(true, false);
    }

    @Test
    public void when_groupForceDestroyed_afterLeaderElection_then_itsDirShouldBeDeleted() throws Exception {
        when_groupDestroyed_then_itsDirShouldBeDeleted(true, true);
    }

    private void when_groupDestroyed_then_itsDirShouldBeDeleted(boolean forceDestroy, boolean waitLeaderElection)
            throws Exception {
        assumeThat("This test does not involve restart", restartAddressPolicy, equalTo(AddressPolicy.REUSE_EXACT));

        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);

        for (int i = 0; i < 3; i++) {
            RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
            RaftGroupId group = invocationManager.createRaftGroup("group" + i).join();

            if (waitLeaderElection) {
                waitAllForLeaderElection(instances, group);
            }

            if (forceDestroy) {
                getRaftService(instances[0]).forceDestroyCPGroup(group.getName()).get();
            } else {
                destroyRaftGroup(instances[0], group);
            }

            assertTrueEventually(() -> {
                for (HazelcastInstance instance : instances) {
                    // TODO: Do we guarantee this?
                    // RaftService raftService = getRaftService(instance);
                    // RaftNode raftNode = raftService.getRaftNode(group);
                    // assertNull(raftNode);

                    CPPersistenceServiceImpl cpPersistenceService = getCpPersistenceService(instance);
                    File groupDir = cpPersistenceService.getGroupDir(group);
                    assertFalse(group + " directory '" + groupDir + "' should not exist!", groupDir.exists());
                }

                RaftService raftService = getRaftService(instances[0]);
                Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
                assertThat("Groups: " + groupIds, groupIds, hasSize(1));
                assertThat(groupIds, not(hasItem(group)));
                assertNull(raftService.getCPGroup(group.getName()).get());
            });
        }
    }

    @Test
    public void when_groupDestroyed_then_itShouldNotBeRestored() throws Exception {
        when_groupDestroyed_then_itShouldNotBeRestored(false);
    }

    @Test
    public void when_groupForceDestroyed_then_itShouldNotBeRestored() throws Exception {
        when_groupDestroyed_then_itShouldNotBeRestored(true);
    }

    private void when_groupDestroyed_then_itShouldNotBeRestored(boolean forceDestroy) throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        for (int i = 0; i < 3; i++) {
            RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
            RaftGroupId group1 = invocationManager.createRaftGroup("group1").join();
            RaftGroupId group2 = invocationManager.createRaftGroup("group2").join();

            if (forceDestroy) {
                getRaftService(instances[0]).forceDestroyCPGroup(group2.getName()).get();
            } else {
                destroyRaftGroup(instances[0], group2);
            }

            factory.terminateAll();

            instances = restartInstances(addresses, config);

            HazelcastInstance[] finalInstances = instances;
            assertTrueEventually(() -> {
                for (HazelcastInstance instance : finalInstances) {
                    RaftService raftService = getRaftService(instance);
                    assertNull(raftService.getRaftNode(group2));

                    CPPersistenceServiceImpl cpPersistenceService = getCpPersistenceService(instance);
                    File groupDir = cpPersistenceService.getGroupDir(group2);
                    assertFalse(group2 + " directory '" + groupDir + "' should not exist!", groupDir.exists());
                }

                RaftService raftService = getRaftService(finalInstances[0]);
                Collection<CPGroupId> groupIds = raftService.getCPGroupIds().get();
                assertThat("Groups: " + groupIds, groupIds, hasSize(2));
                assertThat(groupIds, hasItem(group1));
                assertThat(groupIds, not(hasItem(group2)));

                assertEquals(group1, raftService.getCPGroup(group1.getName()).get().id());
                assertNull(raftService.getCPGroup(group2.getName()).get());
            });
        }
    }

    private CPPersistenceServiceImpl getCpPersistenceService(HazelcastInstance instance) {
        return (CPPersistenceServiceImpl) getNode(instance).getNodeExtension().getCPPersistenceService();
    }

    private void destroyRaftGroup(HazelcastInstance instance, CPGroupId groupId) {
        RaftService service = getRaftService(instance);
        RaftInvocationManager invocationManager = service.getInvocationManager();
        invocationManager.invoke(service.getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).join();
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

    @Test
    public void when_cpMemberRestartsWithoutCPMemberFile_then_restartFails() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().shutdown();

        ILogger logger = instances[1].getLoggingService().getLogger(getClass());

        File[] memberDirs = baseDir.listFiles((dir, name) -> dir.isDirectory());
        assertNotNull(memberDirs);

        Optional<File> memberDirOpt = Arrays.stream(memberDirs).filter(dir -> {
            try {
                DirectoryLock lock = DirectoryLock.lockForDirectory(dir, logger);
                lock.release();
                return true;
            } catch (Exception e) {
                return false;
            }
        }).findFirst();

        assertTrue(memberDirOpt.isPresent());

        IOUtil.delete(new File(memberDirOpt.get(), CPMetadataStoreImpl.CP_MEMBER_FILE_NAME));

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart with missing CP member file!");
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_cpMemberRestartsWithAPMemberFile_then_restartFails() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().shutdown();

        ILogger logger = instances[1].getLoggingService().getLogger(getClass());

        File[] memberDirs = baseDir.listFiles((dir, name) -> dir.isDirectory());
        assertNotNull(memberDirs);

        Optional<File> memberDirOpt = Arrays.stream(memberDirs).filter(dir -> {
            try {
                DirectoryLock lock = DirectoryLock.lockForDirectory(dir, logger);
                lock.release();
                return true;
            } catch (Exception e) {
                return false;
            }
        }).findFirst();

        assertTrue(memberDirOpt.isPresent());

        File cpMemberFile = new File(memberDirOpt.get(), CPMetadataStoreImpl.CP_MEMBER_FILE_NAME);
        IOUtil.delete(cpMemberFile);
        boolean created = cpMemberFile.createNewFile();
        assertTrue(created);

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart with missing CP member file!");
        } catch (IllegalStateException ignored) {
        }
    }

}
